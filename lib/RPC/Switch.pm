package RPC::Switch;

#
# Mojo's default reactor uses EV, and EV does not play nice with signals
# without some handholding. We either can try to detect EV and do the
# handholding, or try to prevent Mojo using EV.
#
BEGIN {
	#$ENV{'MOJO_REACTOR'} = 'Mojo::Reactor::Poll';
}
# we do the handholding now..

# mojo (from cpan)
use Mojo::Base -strict;
use Mojo::File;
use Mojo::IOLoop;
use Mojo::Log;

# standard
use Carp;
use Cwd qw(realpath);
use Data::Dumper;
use Digest::MD5 qw(md5_base64);
use Encode qw(encode_utf8 decode_utf8);
use English;
use Fcntl qw(:DEFAULT);
use File::Basename;
use File::Path qw(remove_tree);
use File::Temp qw(tempfile);
use FindBin qw($RealBin $RealScript);
use List::Util qw(any shuffle);
use POSIX qw(:sys_wait_h);
use Scalar::Util qw(refaddr);
use Time::HiRes qw(sleep);

# more cpan
use CBOR::XS; # or Sereal?
#use Clone::PP qw(clone);
use JSON::MaybeXS; # fixme: directly use Cpanel::Mojo::XS?
use LMDB_File qw(:cursor_op :error :flags);
#use Mojo::SQLite;
use MojoX::LineStream;
use MojoX::POSIX_RT_MQ;
use Ref::Util qw(is_arrayref is_coderef is_hashref);
#use Sereal::Decoder; # or CBOR::XS?
#use Sereal::Encoder;

# RPC Switch aka us
use RPC::Switch::Auth;
use RPC::Switch::Channel;
use RPC::Switch::Connection;
use RPC::Switch::MQ;
use RPC::Switch::Processor;
use RPC::Switch::ReqAuth;
use RPC::Switch::Server;
use RPC::Switch::Shared;
use RPC::Switch::WorkerMethod;

#use constant MAXMQSIZE = 8000;

# lotsa globals
our (
	$cfg,
	@children,
	$chunks,
	$concount,
	%cons,
	$decoder,
	$debug,
	$encoder,
	@heartbeat,
	$ioloop,
	$last_worker_id,
	$log,
	$methodpath,
	@mq,
	$numproc,
	%pids,
	$ping,
	$rundir,
	%servers,
	$timeout,
	$tmpdir,
);

sub switch {
	my (%args) = @_;

	my $cfgdir = $args{cfgdir};
	die "no configdir?" unless $cfgdir;
	my $cfgfile = $args{cfgfile} // 'rpcswitch.conf';
	my $cfgpath = "$cfgdir/$cfgfile";

	my $slurp = Mojo::File->new($cfgpath)->slurp();
	local $@;
	eval $slurp;
	die "failed to load config $cfgpath: $@\n" if $@;
	die "empty config $cfgpath?" unless is_hashref($cfg);

	$debug = $args{debug}; # // 1;

	$log = $args{log} // Mojo::Log->new(level => ($debug) ? 'debug' : 'info');
	$log->path($args{logfile}) if $args{logfile};

	_tmpdir();
	RPC::Switch::Shared::create_db(debug => $debug, log => $log, rundir => $tmpdir);
	# todo: cleanup on die?

	# exlicitly initialize everything here so that a re-init has a chance of working
	# keep sorted
	# todo: move these to processor?
	$chunks = 0; # how many json chunks we handled
	$concount = 0; # how many connections we've had
	$ioloop = $args{ioloop} // Mojo::IOLoop->singleton;
	$last_worker_id = 0; # last assigned worker_id
	$numproc = $cfg->{processors} // 2;
	$ping = $args{ping} || 60;
	%servers = ();
	$timeout = $args{timeout} // 60; # 0 is a valid timeout?

	#$auth = RPC::Switch::Auth->new(
	RPC::Switch::Auth::init(
		$cfgdir, $cfg, 'auth',
	) or die 'no auth?';

	RPC::Switch::ReqAuth::init(
		cfgdir => $cfgdir,
		cfg => $cfg,
		cfgsection => 'reqauth',
		ioloop => $ioloop,
	) or die 'no reqauth?';

	# do this after requath init so that we can check
	my $methodcfg = $cfg->{methods} or die 'no method configuration?';
	$methodpath = "$cfgdir/$methodcfg";
	eval {
		_load_config();
	};
	if ($@) {
		$log->error("loading method config failed: $@");
		goto CLEANUP;
	}
	# todo: catch errors and do cleanup?

	#$decoder = Sereal::Decoder->new();
	#$encoder = Sereal::Encoder->new();
	$decoder = $encoder = CBOR::XS->new;

	# share vars
	*RPC::Switch::Connection::chunks = \$chunks;
	*RPC::Switch::Connection::concount = \$concount;
	*RPC::Switch::Connection::debug = \$debug;
	*RPC::Switch::Connection::ioloop = \$ioloop;
	*RPC::Switch::Connection::log = \$log;

	#*RPC::Switch::Processor::auth = \$auth;
	*RPC::Switch::Processor::chunks = \$chunks;
	*RPC::Switch::Processor::concount = \$concount;
	*RPC::Switch::Processor::cons = \%cons;
	*RPC::Switch::Processor::debug = \$debug;
	*RPC::Switch::Processor::decoder = \$decoder;
	*RPC::Switch::Processor::encoder = \$encoder;
	*RPC::Switch::Processor::ioloop = \$ioloop;
	*RPC::Switch::Processor::log = \$log;
	*RPC::Switch::Processor::mq = \@mq;
	*RPC::Switch::Processor::numproc = \$numproc;
	*RPC::Switch::Processor::tmpdir = \$tmpdir;

	*RPC::Switch::Server::cons = \%cons;
	*RPC::Switch::Server::ioloop = \$ioloop;

	# init processor before fork
	RPC::Switch::Processor::_init();

	die "no listen configuration?" unless is_arrayref($cfg->{listen});
	#my @servers;
	for my $l (@{$cfg->{listen}}) {
		my $s = RPC::Switch::Server->new($l);
		$servers{$s->id} = $s;
	}

	# any new server re-enables all stopped servers, so we stop them all
	# once we're done creating
	$_->server->stop() for values %servers;

	if ($ioloop->reactor->isa('Mojo::Reactor::EV')) {
		$log->info('Mojo::Reactor::EV detected, enabling workarounds');
		#Mojo::IOLoop->recurring(1 => sub {
		#	$log->debug('--tick--') if $debug
		#});
		$RPC::Switch::__async_check = EV::check(sub {
			#$log->debug('--tick--') if $debug;
			1;
		});
	}

	# start child processor processes after setting up the listening sockets
	_start_processors();

	local $SIG{TERM} = local $SIG{INT} = \&_shutdown;

	local $SIG{CHLD} = sub {
		local ($!, $?);
		my $flag;
		while ((my $p = waitpid(-1, WNOHANG)) > 0) {
			if (my $c = delete $pids{$p}) {
				$log->error("child $c ($p) died: $?");
				$children[$c] = undef;
				$flag = 1;
			}
		}
		$ioloop->stop if $flag and $ioloop->is_running;
	};

	local $SIG{HUP} = sub {
		$log->info('trying to reload config');
		local $@;
		eval {
			_load_config();
		};
		if ($@) {
			$log->error("config reload failed: $@");
		} else {
			$log->error("config reloaded succesfully");
		}
	};

	$log->info("RPC::Switch master [$$] starting work");
	$ioloop->start unless $ioloop->is_running;

	CLEANUP:
	$log->info("RPC::Switch master [$$] starting cleanup");

	kill 'TERM', map { $log->info("killing $_"); $_ } grep defined, @children;
	my $sleep=0;
	do {
		sleep(.1); # hack
		$sleep++;
		$log->info( 'children: ' . join(', ', (map { $_ // '_'  } @children)));
	} while grep defined, @children and $sleep < 3;

	RPC::Switch::Shared::cleanup_db();

	remove_tree($tmpdir, {verbose => 1});

	for (1..$numproc) {
		my $q = $mq[$_] or next;
		$q->unlink();
		$q->close();
	}

	$log->info('RPC::Switch done?');

	return 0;
}

sub _tmpdir {
	my ($path) = @_;
	$path //= (-d "/run/user/$EUID/") ? "/run/user/$EUID/" :
		((-d '/dev/shm/') ? '/dev/shm' : '/tmp');

	$tmpdir = "$path/$RealScript-$$";
	mkdir "$tmpdir" or die "could not mkdir $tmpdir: $!";
	mkdir "$tmpdir/db" or die "could not mkdir $tmpdir/db: $!";
	mkdir "$tmpdir/st" or die "could not mkdir $tmpdir/st: $!";
	mkdir "$tmpdir/xl" or die "could not mkdir $tmpdir/xl: $!";
	say "tmpdir: $tmpdir";
	return $tmpdir;
}

sub _start_processors {

	# create queues
	for my $child (1..$numproc) {
		my $qname = "/q-$RealScript-$$-$child";
		#my $q = MojoX::POSIX_RT_MQ->new(
		my $q = RPC::Switch::MQ->new(
			name => $qname,
			flag => O_RDWR | O_CREAT | O_EXCL,
			mode => 0600,
			attr => { mq_maxmsg => 8, mq_msgsize => 4096 }, #  todo config
			debug => $debug,
			log => $log,
		);
		$q->{rdr} = 0; # only in the child
		#$q->{wtr} = 0; ??
		$q->{cid} = $q->{workername} = "q-$child";
		$q->{channels} = {};
		$q->{is_mq} = 1;
		$q->catch(sub {
			$log->error("mq $qname in $child caught $_[0]");
		});
		$mq[$child] = $q;
	}

	 # Pipe for subprocess communication
	pipe(my $reader, my $writer) or die "Can't create pipe: $!";

	# fork of processors
	for my $child (1..$numproc) {
		die "Can't fork: $!" unless defined(my $pid = fork);
		unless ($pid) {	# Child
			$log->debug("in processor child $child pid $$");
			close $reader;

			my $q = $mq[$child];
			$q->{rdr} = 1;

			# (re)open lmdb
			RPC::Switch::Shared::reopen_db();

			#$0 = "$RealScript [$child]";
			$0 .= "[$child]";

			$RPC::Switch::Processor::child = $child;

			# signal succesfull startup to the parent
			#say $writer 'yoohoo!';
			#$writer->flush;

			my $heartbeat_interval = 60; # todo: config
			$ioloop->recurring($heartbeat_interval => sub {
				say $writer "$child $$ ok";
				$writer->flush;
				RPC::Switch::Processor::_write_stats();
			});

			for (values %servers) {
				$log->debug("starting server $$ $_");
				$_->server->start();
			}

			$q->on(msg => \&RPC::Switch::Processor::_handle_q_request);
			$q->start();

			local $SIG{TERM} = local $SIG{INT} = \&_shutdown_child;

			$log->info("RPC::Switch processor $child [$$] starting work");
			$ioloop->start;
			$log->info("RPC::Switch processor $child [$$] starting cleanup");

			$q->close();

			# don't ever return
			exit(0);
		}

		#my $success = readline $reader;
		#die "unable to fork worker!?" unless $success;

		$heartbeat[$child] = time();
		$children[$child] = $pid;
		$pids{$pid} = $child;
	}

	close $writer;
	my $rdrstream = Mojo::IOLoop::Stream->new($reader)->timeout(0);
	my $rdrlines = MojoX::LineStream->new(stream => $rdrstream);
	$ioloop->stream($rdrstream);

	$rdrlines->on(line => sub {
		my ($ls, $line) = @_;
		my ($child, $pid, $msg) = split ' ', $line, 3;
		$log->debug("got line from $child [$pid]: $msg");
		$heartbeat[$child] = time();
	});
	$rdrlines->on(close => sub {
		$log->error("got close!?");
		#whut?
	});
	# more?

	return;
}

sub _load_config {
	die 'no methodpath?' unless $methodpath;

	my $slurp = Mojo::File->new($methodpath)->slurp();

	my ($acl, $backend2acl, $backendfilter, $method2acl, $methods);

	local $SIG{__WARN__} = sub { die @_ };

	eval $slurp;

	die "error loading method config: $@" if $@;
	die 'emtpy method config?' unless $acl && $backend2acl
		&& $backendfilter && $method2acl && $methods;

	# reverse the acl hash: create a hash of users with a hash of acls
	# these users belong to as values
	my %who2acl;
	while (my ($a, $b) = each(%$acl)) {
		#say 'processing ', $a;
		my @acls = ($a, 'public');
		my @users;
		my $i = 0;
		my @tmp = (is_arrayref($b) ? @$b : ($b));
		while ($_ = shift @tmp) {
			#say "doing $_";
			if (/^\+(.*)$/) {
				#say "including acl $1";
				die "acl depth exceeded for $1" if ++$i > 10;
				my $b2 = $acl->{$1};
				die "unknown acl $1" unless $b2;
				push @tmp, (is_arrayref($b2) ? @$b2 : $b2);
			} else {
				push @users, $_
			}
		}
		#print 'acls: ', Dumper(\@acls);
		#print 'users: ', Dumper(\@users);
		# now we have a list of acls resolving to a list o users
		# for all users in the list of users
		for my $u (@users) {
			# add the acls
			if ($who2acl{$u}) {
				$who2acl{$u}->{$_} = 1 for @acls;
			} else {
				$who2acl{$u} = { map { $_ => 1} @acls };
			}
		}
	}
	while (my ($a, $b) = each(%who2acl)) {
		ins('who2acl', $a, $b);
	}

	# check if all acls mentioned exist
	while (my ($a, $b) = each(%$backend2acl)) {
		$b = [ $b ] unless ref $b;
		for my $c (@$b) {
			die "acl $c unknown for backend $a" unless $acl->{$c};
		}
		ins('backend2acl', $a, $b);
	}

	while (my ($a, $b) = each(%$backendfilter)) {
		ins('backendfilter', $a, $b);
	}

	while (my ($a, $b) = each(%$method2acl)) {
		$b = [ $b ] unless ref $b;
		for my $c (@$b) {
			die "acl $c unknown for method $a" unless $acl->{$c};
		}
		ins('method2acl', $a, $b);
	}

	# namespace magic
	my %methods;
	while (my ($ns, $ms) = each(%$methods)) {
		while (my ($m, $md) = each(%$ms)) {
			$md = { b => $md } if not ref $md;
			my $fm = "$ns.$m";
			my $be = $md->{b};
			die "invalid metod details for $fm: missing backend"
				unless $be;
			if ($be eq '.') {
				$md->{b} = $be = $fm;
			} elsif ($be =~ '\.$') {
				$md->{b} = $be = "$be$m";
			}
			my ($bns) = split /\./, $be, 2;
			$md->{_a} = $method2acl->{$fm} // $method2acl->{"$ns.*"}
				// die "no acl for method $fm?";
			if (my $bf = $backendfilter->{$be} // $backendfilter->{"$bns.*"}) {
				$md->{_f} = $bf;
			}
			if (my $r = $md->{r}) {
				$md->{r} = $r = [ $r ] unless is_arrayref($r);
				for (@$r) {
					die "reqauth type $_ does not exist"
						unless $RPC::Switch::ReqAuth::types{$_};
				}
			}
			$methods{$fm} = $md;
			ins('methods', $fm, $md);
		}
	}

	if ($debug) {
		$log->debug('acl           ' . Dumper($acl));
		$log->debug('backend2acl   ' . Dumper($backend2acl));
		$log->debug('backendfilter ' . Dumper($backendfilter));
		$log->debug('method2acl    ' . Dumper($method2acl));
		$log->debug('methods       ' . Dumper(\%methods));
		$log->debug('who2acl       ' . Dumper(\%who2acl));
	}

}

sub _shutdown {
	my ($sig) = @_;
	$log->info("parent caught sig$sig, shutting down");
	$ioloop->stop;
}

sub _shutdown_child {
	my ($sig) = @_;
	$log->info("child [$$] caught sig$sig, shutting down");
	$ioloop->stop;
}

1;

__END__

