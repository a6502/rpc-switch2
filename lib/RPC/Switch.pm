package RPC::Switch;

#
# Mojo's default reactor uses EV, and EV does not play nice with signals
# without some handholding. We either can try to detect EV and do the
# handholding, or try to prevent Mojo using EV.
#
#BEGIN {
#	$ENV{'MOJO_REACTOR'} = 'Mojo::Reactor::Poll';
#}
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
use Encode qw(encode_utf8 decode_utf8);
use Digest::MD5 qw(md5_base64);
use File::Basename;
use FindBin;
use List::Util qw(shuffle);
use Scalar::Util qw(refaddr);

# more cpan
use Clone::PP qw(clone);
use JSON::MaybeXS;
use Ref::Util qw(is_arrayref is_coderef is_hashref);

# RPC Switch aka us
use RPC::Switch::Auth;
use RPC::Switch::Channel;
use RPC::Switch::Connection;
use RPC::Switch::Server;
use RPC::Switch::WorkerMethod;

use constant {
	ERR_NOTNOT   => -32000, # Not a notification
	ERR_ERR	     => -32001, # Error thrown by handler
	ERR_BADSTATE => -32002, # Connection is not in the right state (i.e. not authenticated)
	ERR_NOWORKER => -32003, # No worker avaiable
	ERR_BADCHAN  => -32004, # Badly formed channel information
	ERR_NOCHAN   => -32005, # Channel does not exist
	ERR_GONE     => -32006, # Worker gone
	ERR_NONS     => -32007, # No namespace
	ERR_NOACL    => -32008, # Method matches no ACL
	ERR_NOTAL    => -32009, # Method not allowed by ACL
	ERR_BADPARAM => -32010, # No paramters for filtering (i.e. no object)
	ERR_TOOBIG   => -32010, # Req/Resp object too big
	# From http://www.jsonrpc.org/specification#error_object
	ERR_REQ	     => -32600, # The JSON sent is not a valid Request object 
	ERR_METHOD   => -32601, # The method does not exist / is not available.
	ERR_PARAMS   => -32602, # Invalid method parameter(s).
	ERR_INTERNAL => -32603, # Internal JSON-RPC error.
	ERR_PARSE    => -32700, # Invalid JSON was received by the server.
};

# keep in sync with the RPC::Switch::Client
use constant {
	RES_OK => 'RES_OK',
	RES_WAIT => 'RES_WAIT',
	RES_ERROR => 'RES_ERROR',
	RES_OTHER => 'RES_OTHER', # 'dunno'
};

# lotsa globals
our (
	$auth,
	$backend2acl,
	$backendfilter,
	$cfg,
	$chunks,
	$clients,
	$connections,
	$debug,
	$internal,
	$ioloop,
	$last_worker_id,
	$log,
	$method2acl,
	$methodpath,
	$methods,
	$ping,
	$servers,
	$timeout,
	$who2acl,
	$workers,
	$workermethods,
);

sub init {
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

	#print Dumper($cfg);
	my $methodcfg = $cfg->{methods} or die 'no method configuration?';
	$methodpath = "$cfgdir/$methodcfg";
	_load_config();
	die 'method config failed to load?' unless is_hashref($methods);

	# exlicitly initialize everything here so that a re-init has a chance of working
	# keep sorted
	$chunks = 0; # how many json chunks we handled
	$clients = {}; # connected clients
	$connections = 0; # how many connections we've had
	$internal = {}; # rpcswitch.x internal methods
	$ioloop = $args{ioloop} // Mojo::IOLoop->singleton; # laziness
	$last_worker_id = 0; # last assigned worker_id
	$ping = $args{ping} || 60;
	$timeout = $args{timeout} // 60; # 0 is a valid timeout?
	$workers = 0; # count of connected workers
	$workermethods = {}; # announced worker methods

	# announce internal methods
	_register('rpcswitch.announce', \&rpc_announce, non_blocking => 1, state => 'auth');
	_register('rpcswitch.get_clients', \&rpc_get_clients, state => 'auth');
	_register('rpcswitch.get_method_details', \&rpc_get_method_details, state => 'auth');
	_register('rpcswitch.get_methods', \&rpc_get_methods, state => 'auth');
	_register('rpcswitch.get_stats', \&rpc_get_stats, state => 'auth');
	_register('rpcswitch.get_workers', \&rpc_get_workers, state => 'auth');
	_register('rpcswitch.hello', \&rpc_hello, non_blocking => 1);
	_register('rpcswitch.ping', \&rpc_ping);
	_register('rpcswitch.withdraw', \&rpc_withdraw, state => 'auth');

	$auth = RPC::Switch::Auth->new(
		$cfgdir, $cfg, 'auth',
	) or die 'no auth?';

	die "no listen configuration?" unless is_arrayref($cfg->{listen});
	my @servers;
	for my $l (@{$cfg->{listen}}) {
		push @servers, RPC::Switch::Server->new($l);
	}
	$servers = \@servers;

	return JSON->true;
}

sub _load_config {
	die 'no methodpath?' unless $methodpath;

	my $slurp = Mojo::File->new($methodpath)->slurp();

	#my ($acl, $backend2acl, $backendfilter, $method2acl, $methods);
	my $acl;

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

	# check if all acls mentioned exist
	while (my ($a, $b) = each(%$backend2acl)) {
		#my @tmp = ((ref $b eq 'ARRAY') ? @$b : ($b));
		$b = [ $b ] unless ref $b;
		for my $c (@$b) {
			die "acl $c unknown for backend $a" unless $acl->{$c};
		}
	}

	while (my ($a, $b) = each(%$method2acl)) {
		#my @tmp = ((ref $b eq 'ARRAY') ? @$b : ($b));
		$b = [ $b ] unless ref $b;
		for my $c (@$b) {
			die "acl $c unknown for method $a" unless $acl->{$c};
		}
	}

	# namespace magic
	my %methods;
	while (my ($namespace, $ms) = each(%$methods)) {
		while (my ($m, $md) = each(%$ms)) {
			$md = { b => $md } if not ref $md;
			my $be = $md->{b};
			die "invalid metod details for $namespace.$m: missing backend"
				unless $be;
			$md->{b} = "$be$m" if $be =~ '\.$';
			$methods{"$namespace.$m"} = $md;
		}
	}

	if ($debug) {
		$log->debug('acl           ' . Dumper($acl));
		$log->debug('backend2acl   ' . Dumper($backend2acl));
		$log->debug('backendfilter ' . Dumper($backendfilter));
		$log->debug('method2acl    ' . Dumper($method2acl));
		$log->debug('methods       ' . Dumper($methods));
		$log->debug('who2acl       ' . Dumper(\%who2acl));
	}

	$methods = \%methods;
	$who2acl = \%who2acl;
}

sub work {

	if (Mojo::IOLoop->singleton->reactor->isa('Mojo::Reactor::EV')) {
		$log->info('Mojo::Reactor::EV detected, enabling workarounds');
		#Mojo::IOLoop->recurring(1 => sub {
		#	$log->debug('--tick--') if $debug
		#});
		$RPC::Switch::__async_check = EV::check(sub {
			#$log->debug('--tick--') if $debug;
			1;
		});
	}

	local $SIG{TERM} = local $SIG{INT} = \&_shutdown;

	local $SIG{HUP} = sub {
		$log->info('trying to reload config');
		local $@;
		eval {
			_load_config();
		};
		$log->error("config reload failed: $_") if $@;
	};

	$log->info('RPC::Switch starting work');
	$ioloop->start unless $ioloop->is_running;
	$log->info('RPC::Switch done?');

	return 0;
}

sub rpc_ping {
	#my ($c, $r, $i, $rpccb) = @_;
	# fixme: shouldn't this  be
	# return (RES_OK, 'pong?'); 
	return 'pong?';
}

sub rpc_hello {
	my ($con, $r, $args, $rpccb) = @_;
	#$log->debug('rpc_hello: '. Dumper($args));
	my $who = $args->{who} or die "no who?";
	my $method = $args->{method} or die "no method?";
	my $token = $args->{token} or die "no token?";

	$auth->authenticate($method, $con, $who, $token, sub {
		my ($res, $msg, $reqauth) = @_;
		if ($res) {
			$log->info("hello from $who succeeded: method $method msg $msg");
			$con->who($who);
			$con->reqauth($reqauth);
			$con->state('auth');
			$rpccb->(JSON->true, "welcome to the rpcswitch $who!");
		} else {
			$log->info("hello failed for $who: method $method msg $msg");
			$con->state(undef);
			# close the connecion after sending the response
			$ioloop->next_tick(sub {
				$con->close;
			});
			$rpccb->(JSON->false, 'you\'re not welcome!');
		}
	});
}

sub rpc_get_clients {
	my ($con, $r, $i) = @_;

	#my $who = $con->who;
	# fixme: acl for this?
	my %res;

	for my $c ( values %$clients ) {
		$res{$c->{from}} = {
			localname => $c->{server}->{localname},
			(($c->{methods} && %{$c->{methods}}) ? (methods => [keys %{$c->{methods}}]) : ()),
			num_chan => scalar keys %{$c->{channels}},
			who => $c->{who},
			($c->{workername} ? (workername => $c->{workername}) : ()),
		}
	}

	# follow the rpc-switch calling conventions here
	return (RES_OK, \%res);
}

sub rpc_get_method_details {
	my ($con, $r, $i, $cb) = @_;

	my $who = $con->who;

	my $method = $i->{method} or die 'method required';

	my $md = $methods->{$method}
		or die "method $method not found";

	$method =~ /^([^.]+)\..*$/
		or die "no namespace in $method?";
	my $ns = $1;

	my $acl = $method2acl->{$method}
		  // $method2acl->{"$ns.*"}
		  // die "no method acl for $method";

	die "acl $acl does not allow calling of $method by $who"
		unless checkacl($acl, $con->who);

	$md = clone($md); # copy to clobber

	# now find a backend
	my $backend = $md->{b};

	my $l = $workermethods->{$backend};

	if ($l) {
		if (is_hashref($l)) {
			my $dummy;
			# filtering
			($dummy, $l) = each %$l;
		}
		if (is_arrayref($l) and @$l) {
			my $wm = $$l[0];
			$md->{doc} = $wm->{doc}
				// 'no documentation available';
		}
	} else {
		$md->{msg} = 'no backend worker available';
	}


	# follow the rpc-switch calling conventions here
	return (RES_OK, $md);
}

sub rpc_get_methods {
	my ($con, $r, $i) = @_;

	my $who = $con->who;
	my @m;

	for my $method ( keys %$methods ) {
		$method =~ /^([^.]+)\..*$/
			or next;
		my $ns = $1;
		my $acl = $method2acl->{$method}
			  // $method2acl->{"$ns.*"}
			  // next;

		next unless _checkacl($acl, $who);

		push @m, { $method => ( $methods->{$method}->{d} // 'undocumented method' ) };
	}

	# follow the rpc-switch calling conventions here
	return (RES_OK, \@m);
}

sub rpc_get_stats {
	my ($con, $r, $i) = @_;

	#my $who = $con->who;
	# fixme: acl for stats?

	keys %$methods; #reset
	my ($k, $v, %m);
	while ( ($k, $v) = each(%$methods) ) {
		$v = $v->{'#'};
		$m{$k} = $v if $v;
	}

	my %stats = (
		chunks => $chunks,
		clients => scalar keys %$clients,
		connections => $connections,
		workers => $workers,
		methods => \%m,
	);

	# follow the rpc-switch calling conventions here
	return (RES_OK, \%stats);
}

sub rpc_get_workers {
	my ($con, $r, $i) = @_;

	#my $who = $con->who;
	# fixme: acl for this?

	#print 'workermethods: ', Dumper($workermethods);
	my %workers;

	for my $l ( values %$workermethods ) {
		if (ref $l eq 'ARRAY' and @$l) {
			#print 'l : ', Dumper($l);
			for my $wm (@$l) {
				push @{$workers{$wm->connection->workername}}, $wm->method;
			}
		} elsif (ref $l eq 'HASH') {
			# filtering
			keys %$l; # reset each
			while (my ($f, $wl) = each %$l) {
				for my $wm (@$wl) {
					push @{$workers{$wm->connection->workername}}, [$wm->method, $f];
				}
			}
		}
	}

	# follow the rpc-switch calling conventions here
	return (RES_OK, \%workers);
}

# kept as a debuging reference..
sub _checkacl_slow {
	my ($acl, $who) = @_;
	
	#$acl = [$acl] unless ref $acl;
	say "check if $who is in any of ('",
		(ref $acl ? join("', '", @$acl) : $acl), "')";

	my $a = $who2acl->{$who} // { public => 1 };
	say "$who is in ('", join("', '", keys %$a), "')";

	#return scalar grep(defined, @{$a}{@$acl}) if ref $acl;
	if (ref $acl) {
		my @matches = grep(defined, @{$a}{@$acl});
		print  'matches: ', Dumper(\@matches);
		return scalar @matches;
	}
	return $a->{$acl};
}

sub _checkacl {
	my $a = $who2acl->{$_[1]} // { public => 1 };
	return scalar grep(defined, @{$a}{@{$_[0]}}) if ref $_[0];
	return $a->{$_[0]};
}

sub rpc_announce {
	my ($con, $req, $i, $rpccb) = @_;
	my $method = $i->{method} or die 'method required';
	my $who = $con->who;
	$log->info("announce of $method from $who ($con->{from})");
	my $workername = $i->{workername} // $con->workername // $con->who;
	my $filter     = $i->{filter};
	my $worker_id = $con->worker_id;
	unless ($worker_id) {
		# it's a new worker: assign id
		$worker_id = ++$last_worker_id;
		$con->worker_id($worker_id);
		$workers++; # and count
	}
	$log->debug("worker_id: $worker_id");

	# check if namespace.method matches a backend2acl (maybe using a namespace.* wildcard)
	# if not: fail
	# check if $client->who appears in that acl

	$method =~ /^([^.]+)\..*$/ 
		or die "no namespace in $method?";
	my $ns = $1;

	my $acl = $backend2acl->{$method}
		  // $backend2acl->{"$ns.*"}
		  // die "no backend acl for $method";

	die "acl $acl does not allow announce of $method by $who"
		unless _checkacl($acl, $con->who);

	# now check for filtering

	my $filterkey = $backendfilter->{$method}
			// $backendfilter->{"$ns.*"};

	my $filtervalue;

	if ( $filterkey ) {
		$log->debug("looking for filterkey $filterkey");
		if ($filter) {
			die "filter should be a json object" unless is_arrayref($filter);
			for (keys %$filter) {
				die "filtering is not allowed on field $_ for method $method"
					unless '' . $_ eq $filterkey;
				$filtervalue = $filter->{$_};
				die "filtering on a undefined value makes little sense"
					unless defined $filtervalue;
				die "filtering is only allowed on simple values"
					if ref $filtervalue;
				# do something here?
				#$filtervalue .= ''; # force string context?
			}
		} else {
			die "filtering is required for method $method";
		}
	} elsif ($filter) {
		die "filtering not allowed for method $method";
	}

	my $wm = RPC::Switch::WorkerMethod->new(
		method => $method,
		connection => $con,
		doc => $i->{doc},
		($filterkey ? (
			filterkey => $filterkey,
			filtervalue => $filtervalue
		) : ()),
	);

	die "already announced $wm" if $con->methods->{$method};
	$con->methods->{$method} = $wm;

	$con->workername($workername) unless $con->workername;

	if ($filterkey) {
		push @{$workermethods->{$method}->{$filtervalue}}, $wm
	} else {
		push @{$workermethods->{$method}}, $wm;
	}

	# set up a ping timer to the client after the first succesfull announce
	unless ($con->tmr) {
		$con->{tmr} = $ioloop->recurring( $con->ping, sub { _ping($con) } );
	}
	$rpccb->(JSON->true, { msg => 'success', worker_id => $worker_id });

	# fixme: make non-async?
	return;
}

sub rpc_withdraw {
	my ($con, $m, $i) = @_;
	my $method = $i->{method} or die 'method required';

	my $wm = $con->methods->{$method} or die "unknown method";
	# remove this action from the clients action list
	delete $con->methods->{$method};

	if (not %{$con->methods} and $con->{tmr}) {
		# cleanup ping timer if client has no more actions
		$log->debug("remove tmr $con->{tmr}");
		$ioloop->remove($con->{tmr});
		delete $con->{tmr};
		# and reduce worker count
		$workers--;
	}

	# now remove this workeraction from the listenstring workeraction list

	my $wmh = $workermethods; # laziness
	my $l;
	if (my $fv = $wm->filtervalue) {
		$l = $wmh->{$method}->{$fv};
		if ($#$l) {
			my $rwm = refaddr $wm;
			splice @$l, $_, 1 for grep(refaddr $$l[$_] == $rwm, 0..$#$l);
			delete $wmh->{$method}->{$fv} unless @$l;
		} else {
			delete $wmh->{$method}->{$fv};
		}
		delete $wmh->{$method} unless
			%{$wmh->{$method}};

	} else {
		$l = $wmh->{$method};

		if ($#$l) {
			my $rwm = refaddr $wm;
			splice @$l, $_, 1 for grep(refaddr $$l[$_] == $rwm, 0..$#$l);
			delete $wmh->{$method} unless @$l;
		} else {
			delete $wmh->{$method};
		}

	}

	#print 'workermethods: ', Dumper($wmh);

	return 1;
}

sub _ping {
	my ($con) = @_;
	my $tmr;
	$ioloop->delay(sub {
		my $d = shift;
		my $e = $d->begin;
		$tmr = $ioloop->timer(10 => sub { $e->(@_, 'timeout') } );
		$con->call('rpcswitch.ping', {}, sub { $e->($con, @_) });
	},
	sub {
		my ($d, $e, $r) = @_;
		#print  'got ', Dumper(\@_);
		if ($e and $e eq 'timeout') {
			$log->info('uhoh, ping timeout for ' . $con->who);
			$ioloop->remove($con->id); # disconnect
		} else {
			$ioloop->remove($tmr);
			if ($e) {
				$log->debug("'got $e->{message} ($e->{code}) from $con->{who}");
				return;
			}
			$log->debug('got ' . $r . ' from ' . $con->who . ' : ping(' . $con->worker_id . ')');
		}
	});
}

sub _shutdown {
	my ($sig) = @_;
	$log->info("caught sig$sig, shutting down");

	# todo: cleanup

	$ioloop->stop;
}

# register internal rpcswitch.* methods
sub _register {
	my ($name, $cb, %opts) = @_;
	my %defaults = ( 
		by_name => 1,
		non_blocking => 0,
		notification => 0,
		raw => 0,
		state => undef,
	);
	croak 'no callback?' unless is_coderef($cb);
	%opts = (%defaults, %opts);
	croak 'a non_blocking notification is not sensible'
		if $opts{non_blocking} and $opts{notification};
	croak "internal methods need to start with rpcswitch." unless $name =~ /^rpcswitch\./;
	croak "method $name already registered" if $internal->{$name};
	$internal->{$name} = { 
		name => $name,
		cb => $cb,
		by_name => $opts{by_name},
		non_blocking => $opts{non_blocking},
		notification => $opts{notification},
		raw => $opts{raw},
		state => $opts{state},
	};
}

sub _handle_internal_request {
	my ($c, $r) = @_;
	my $m = $internal->{$r->{method}};
	my $id = $r->{id};
	return _error($c, $id, ERR_METHOD, 'Method not found.') unless $m;
	my $params = $r->{params};

	#$log->debug('	m: ' . Dumper($m));
	return _error($c, $id, ERR_NOTNOT, 'Method is not a notification.') if !$id and !$m->{notification};

	return _error($c, $id, ERR_REQ, 'Invalid Request: params should be array or object.')
		unless is_arrayref($params) or is_hashref($params);

	return _error($c, $id, ERR_PARAMS, 'This method expects '.($m->{by_name} ? 'named' : 'positional').' params.')
		if ref $params ne ($m->{by_name} ? 'HASH' : 'ARRAY');
	
	return _error($c, $id, ERR_BADSTATE, 'This method requires connection state ' . ($m->{state} // 'undef'))
		if $m->{state} and not ($c->state and $m->{state} eq $c->state);

	if ($m->{raw}) {
		my $cb;
		$cb = sub { $c->write(encode_json($_[0])) if $id } if $m->{non_blocking};

		local $@;
		#my @ret = eval { $m->{cb}->($c, $jsonr, $r, $cb)};
		my @ret = eval { $m->{cb}->($c, $r, $cb)};
		return _error($c, $id, ERR_ERR, "Method threw error: $@") if $@;
		#say STDERR 'method returned: ', Dumper(\@ret);

		$c->write(encode_json($ret[0])) if !$cb and $id;
		return
	}

	my $cb;
	$cb = sub { _result($c, $id, \@_) if $id; } if $m->{non_blocking};

	local $@;
	my @ret = eval { $m->{cb}->($c, $r, $params, $cb)};
	return _error($c, $id, ERR_ERR, "Method threw error: $@") if $@;
	#$log->debug('method returned: '. Dumper(\@ret));
	
	return _result($c, $id, \@ret) if !$cb and $id;
	return;
}

sub _handle_request {
	my ($c, $request) = @_;
	$log->debug('    in handle_request') if $debug;
	my $method = $request->{method} or die 'huh?';
	my $id = $request->{id};

	my $md;
	unless ($md = $methods->{$method}) {
		# try it as an internal method then
		return _handle_internal_request($c, $request);
	}

	$log->debug("rpc_catchall for $method") if $debug;

	# auth only beyond this point
	return _error($c, $id, ERR_BADSTATE, 'This method requires an authenticated connection')
		unless $c->{state} and 'auth' eq $c->{state};

	# check if $c->who is in the method2acl for this method?
	my ($ns) = split /\./, $method, 2;
	return _error($c, $id, ERR_NONS, "no namespace in $method?")
		unless $ns;

	my $acl = $method2acl->{$method}
		  // $method2acl->{"$ns.*"}
		  // return _error($c, $id, ERR_NOACL, "no method acl for $method");

	my $who = $c->{who};
	return _error($c, $id, ERR_NOTAL, "acl $acl does not allow method $method for $who")
		unless _checkacl($acl, $who);

	#print  'workermethods: ', Dumper($workermethods);

	my $backend = $md->{b};
	my $l; 

	if (my $fk = $backendfilter->{$backend}
			// $backendfilter->{"$ns.*"}) {

		$log->debug("filtering for $backend with $fk") if $debug;
		my $p = $request->{params};
		return _error($c, $id, ERR_BADPARAM, "Parameters should be a json object for filtering.")
			unless is_hashref($p);

		my $fv = $p->{$fk};

		return _error($c, $id, ERR_BADPARAM, "filter parameter $fk undefined.")
			unless defined($fv);

		$l = $workermethods->{$backend}->{$fv};

		return _error($c, $id, ERR_NOWORKER,
			"No worker available after filtering on $fk for backend $backend")
				unless $l and @$l;
	} else {
		$l = $workermethods->{$backend};
		return _error($c, $id, ERR_NOWORKER, "No worker available for $backend.")
			unless $l and @$l;
	}

	#print  'l: ', Dumper($l);

	my $wm;
	if ($#$l) { # only do expensive calculations when we have to
		# rotate workermethods
		push @$l, shift @$l;
		# sort $l by refcount
		$wm = (sort { $a->{connection}->{refcount} <=> $b->{connection}->{refcount}} @$l)[0];
		# this should produce least refcount round robin balancing
	} else {
		$wm = $$l[0];
	}
	return _error($c, $id, ERR_INTERNAL, 'Internal error.') unless $wm;

	my $wcon = $wm->{connection};
	$log->debug("forwarding $method to $wcon->{workername} ($wcon->{worker_id}) for $backend")
		if $debug;

	# find or create channel
	my $vci = md5_base64(refaddr($c).':'.refaddr($wcon)); # should be unique for this instance?
	my $channel;
	unless ($channel = $c->{channels}->{$vci}) {
		$channel = RPC::Switch::Channel->new(
			client => $c,
			vci => $vci,
			worker => $wcon,
			refcount => 0,
			reqs => {},
		);
		$c->channels->{$vci} =
			$wcon->channels->{$vci} =
				$channel;
	}
	if ($id) {
		$wcon->{refcount}++;
		$channel->{reqs}->{$id} = 1;
	}
	$md->{'#'}++; # per method call stats

	# rewrite request to add rcpswitch information
	my $workerrequest = encode_json({
		jsonrpc => '2.0',
		rpcswitch => {
			vcookie => 'eatme', # channel information version
			vci => $vci,
			who => $who,
		},
		method => $backend,
		params => $request->{params},
		id  => $id,
	});

	# forward request to worker
	if ($debug) {
		$log->debug("refcount connection $wcon $wcon->{refcount}");
		$log->debug("refcount channel $channel " . scalar keys %{$channel->reqs});
		$log->debug('    writing: ' . decode_utf8($workerrequest));
	}

	#$wcon->_write(encode_json($workerrequest));
	$wcon->{ns}->write($workerrequest);
	return; # exlplicit empty return
}

sub _handle_channel {
	my ($c, $jsonr, $r) = @_;
	$log->debug('    in handle_channel') if $debug;
	my $rpcswitch = $r->{rpcswitch};
	my $id = $r->{id};
	
	# fixme: error on a response?
	unless ($rpcswitch->{vcookie}
			 and $rpcswitch->{vcookie} eq 'eatme'
			 and $rpcswitch->{vci}) {
		return _error($c, $id, ERR_BADCHAN, 'Invalid channel information') if $r->{method};
		$log->info("invalid channel information from $c"); # better error message?
		return;
	}

	#print 'rpcswitch: ', Dumper($rpcswitch);
	#print 'channels; ', Dumper($channels);
	my $chan = $c->{channels}->{$rpcswitch->{vci}};
	my $con;
	my $dir;
	if ($chan) {
		if (refaddr($c) == refaddr($chan->{worker})) {
			# worker to client
			$con = $chan->{client};
			$dir = -1;
		} elsif (refaddr($c) == refaddr($chan->{client})) {
			# client to worker
			$con = $chan->{worker};
			$dir = 1;
		} else {
			$chan = undef;
		}
	}		
	unless ($chan) {
		return _error($c, $id, ERR_NOCHAN, 'No such channel.') if $r->{method};
		$log->info("invalid channel from $c");
		return;
	}		
	if ($id) {
		if ($r->{method}) {
			$chan->{reqs}->{$id} = $dir;
		} else {
			$c->{refcount}--;
			delete $chan->{reqs}->{$id};
		}
	}
	if ($debug) {
		$log->debug("refcount connection $c $c->{refcount}");
		$log->debug("refcount $chan " . scalar keys %{$chan->reqs});
		$log->debug('    writing: ' . decode_utf8($$jsonr));
	}
	#print Dumper($chan->reqs);
	# forward request
	# we could spare a encode here if we pass the original request along?
	#$con->_write(encode_json($r));
	$con->{ns}->write($$jsonr);
	return;
}


sub _error {
	my $c = shift;
	return $c->_error(@_);
}

sub _result {
	my ($c, $id, $result) = @_;
	$result = $$result[0] if scalar(@$result) == 1;
	#$log->debug('_result: ' . Dumper($result));
	$c->_write(encode_json({
		jsonrpc	    => '2.0',
		id	    => $id,
		result	    => $result,
	}));
	return;
}

sub _disconnect {
	my ($client) = @_;
	$log->info('oh my.... ' . ($client->who // 'somebody')
				. ' (' . $client->from . ') disonnected..');

	return unless $client->who;

	for my $m (keys %{$client->methods}) {
		$log->debug("withdrawing $m");
		# hack.. fake a rpcswitch.withdraw request
		rpc_withdraw($client, {method => 'withdraw'}, {method => $m});
	}

	for my $c (values %{$client->channels}) {
		#say 'contemplating ', $c;
		my $vci = $c->vci;
		my $reqs = $c->reqs;
		my ($con, $dir);
		if (refaddr $c->worker == refaddr $client) {
			# worker role in this channel: notify client 
			$con = $c->client;
			$dir = 1;
		} else {
			# notify worker
			$con = $c->worker;
			$dir = -1;
		}
		for my $id (keys %$reqs) {
			if ($reqs->{$id} == $dir) {
				$con->_error($id, ERR_GONE, 'opposite end of channel gone');
			} # else ?
		}
		$con->notify('rpcswitch.channel_gone', {channel => $vci});
		delete $con->channels->{$vci};
		#delete $channels->{$vci};
		$c->delete();
	}
	delete $clients->{refaddr($client)};
}


1;

__END__

