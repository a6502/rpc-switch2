package RPC::Switch;

#use strict;
#use warnings;
#use 5.10.0;

#
# Mojo's default reactor uses EV, and EV does not play nice with signals
# without some handholding. We either can try to detect EV and do the
# handholding, or try to prevent Mojo using EV.
#
BEGIN {
	$ENV{'MOJO_REACTOR'} = 'Mojo::Reactor::Poll';
}

# mojo
use Mojo::Base 'Mojo::EventEmitter';
use Mojo::File;
use Mojo::IOLoop;
#use Mojo::JSON qw(decode_json encode_json);
use Mojo::Log;

# standard
use Carp;
use Cwd qw(realpath);
use Data::Dumper;
use Encode qw(encode_utf8 decode_utf8);
use Digest::MD5 qw(md5_base64);
use File::Basename;
use FindBin;
#use IO::Pipe;
use Scalar::Util qw(refaddr);

# cpan
use Config::Tiny;
use JSON::MaybeXS;

# RPC Switch aka us
use RPC::Switch::Auth;
use RPC::Switch::Channel;
use RPC::Switch::Connection;
#use RPC::Switch::JSON::RPC2::TwoWay;
#use RPC::Switch::Job;
#use RPC::Switch::Task;
use RPC::Switch::WorkerMethod;
#use JobCenter::Util qw(:daemon);



has [qw(
	apiname
	auth
	backendacl
	cfg
	channels
	daemon
	debug
	internal
	log
	methodacl
	methods
	pid_file
	ping
	servers
	timeout
	worker_id
	workermethods
)];
#	clients
#	listenstrings
#	pending
#	rpc
#	tasks
#	tmr

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
	# From http://www.jsonrpc.org/specification#error_object
	ERR_REQ	     => -32600, # The JSON sent is not a valid Request object 
	ERR_METHOD   => -32601, # The method does not exist / is not available.
	ERR_PARAMS   => -32602, # Invalid method parameter(s).
	ERR_INTERNAL => -32603, # Internal JSON-RPC error.
	ERR_PARSE    => -32700, # Invalid JSON was received by the server.
};

# keep in sync with RPC::Switch::Client
#use constant {
#	RES_OK => 'RES_OK',
#	RES_WAIT => 'RES_WAIT',
#	RES_ERROR => 'RES_ERROR',
#};

sub new {
	my ($class, %args) = @_;
	my $self = $class->SUPER::new();

	my $cfgdir = $args{cfgdir};
	die "no configdir?" unless $cfgdir;
	my $cfgfile = $args{cfgfile} // 'rpcswitch.conf';
	my $cfgpath = "$cfgdir/$cfgfile";

	my $slurp = Mojo::File->new($cfgpath)->slurp();
	my $cfg;
	local $@;
	eval $slurp;
	die "failed to load config $cfgpath: $@\n" if $@;
	die "empty config $cfgpath?" unless $cfg;
	$self->{cfg} = $cfg;

	my $apiname = $self->{apiname} = ($args{apiname} || fileparse($0)); # . " [$$]";
	my $daemon = $self->{daemon} = $args{daemon} // 0; # or 1?
	my $debug = $self->{debug} = $args{debug} // 0; # or 1?

	my $log = $self->{log} = $args{log} // Mojo::Log->new(level => ($debug) ? 'debug' : 'info');
	$log->path(realpath("$FindBin::Bin/../log/$apiname.log")) if $daemon;

	my $pid_file = $cfg->{pid_file} // realpath("$FindBin::Bin/../log/$apiname.pid");
	die "$apiname already running?" if $daemon and check_pid($pid_file);

	#print Dumper($cfg);
	my $methodcfg = $cfg->{methods} or die 'no method configuration?';
	$self->{methodpath} = "$cfgdir/$methodcfg";
	$self->_load_config();
	die 'method config failed to load?' unless $self->{methods};

	# keep sorted
	$self->{channels} = {};
	$self->{internal} = {}; # rpcswitch.x internal methods
	$self->{pid_file} = $pid_file if $daemon;
	$self->{ping} = $args{ping} || 60;
	$self->{timeout} = $args{timeout} // 60; # 0 is a valid timeout?
	$self->{worker_id} = 0;
	$self->{workermethods} = {}; # announced worker methods

	# announce internal methods
	$self->register('rpcswitch.announce', sub { $self->rpc_announce(@_) }, non_blocking => 1, state => 'auth');
	$self->register('rpcswitch.hello', sub { $self->rpc_hello(@_) }, non_blocking => 1);
	$self->register('rpcswitch.ping', sub { $self->rpc_ping(@_) });
	$self->register('rpcswitch.withdraw', sub { $self->rpc_withdraw(@_) }, state => 'auth');

	die "no listen configuration?" unless ref $cfg->{listen} eq 'ARRAY';
	my @servers;
	for my $l (@{$cfg->{listen}}) {
		my $serveropts = { port => ( $l->{port} // 6551 ) };
		$serveropts->{address} = $l->{address} if $l->{address};
		if ($l->{tls_key}) {
			$serveropts->{tls} = 1;
			$serveropts->{tls_key} = $l->{tls_key};
			$serveropts->{tls_cert} = $l->{tls_cert};
		}
		if ($l->{tls_ca}) {
			#$serveropts->{tls_verify} = 0; # cheating..
			$serveropts->{tls_ca} = $l->{tls_ca};
		}
		my $localname = $l->{name} // (($serveropts->{address} // '0') . ':' . $serveropts->{port});

		my $server = Mojo::IOLoop->server(
			$serveropts => sub {
				my ($loop, $stream, $id) = @_;
				my $client = RPC::Switch::Connection->new($self, $stream, $localname);
				$client->on(close => sub { $self->_disconnect($client) });
				#$self->clients->{refaddr($client)} = $client;
			}
		) or die 'no server?';
		push @servers, $server;
	}
	$self->{servers} = \@servers;

	$self->{auth} = RPC::Switch::Auth->new(
		$cfgdir, $cfg, 'auth',
	) or die 'no auth?';

	# add a catch all error handler..
	$self->catch(sub { my ($self, $err) = @_; warn "This looks bad: $err"; });

	return $self;
}

sub _load_config {
	my ($self) = @_;

	my $path = $self->{methodpath}
		or die 'no methodpath?';

	my $slurp = Mojo::File->new($path)->slurp();

	my ($acl, $backend2acl, $method2acl, $methods);

	local $SIG{__WARN__} = sub { die @_ };

	eval $slurp;

	die "error loading method config: $@" if $@;
	die 'emtpy method config?' unless $acl && $backend2acl && $method2acl && $methods;

	# reverse the acl hash: create a hash of users with a hash of acls
	# these users belong to as values
	my %who2acl;
	while (my ($a, $b) = each(%$acl)) {
		#say 'processing ', $a;
		#next if $done{$a};
		my @acls = ($a, 'public');
		my @users;
		my $i = 0;
		my @tmp = ((ref $b eq 'ARRAY') ? @$b : ($b));
		while ($_ = shift @tmp) {
			#say "doing $_";
			if (/^\+(.*)$/) {
				#say "including acl $1";
				die "acl depth exceeded for $1" if ++$i > 10;
				my $b2 = $acl->{$1};
				die "unknown acl $1" unless $b2;
				push @tmp, ((ref $b2 eq 'ARRAY') ? @$b2 : $b2);
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
		while (my ($m, $b) = each(%$ms)) {
			$b = "$b$m" if $b =~ '\.$';
			$methods{"$namespace.$m"} = $b;
		}
	}

	if ($self->{debug}) {
		my $log = $self->{log};
		$log->debug('acl         ' . Dumper($acl));
		$log->debug('backend2acl ' . Dumper($backend2acl));
		$log->debug('method2acl  ' . Dumper($method2acl));
		$log->debug('methods     ' . Dumper($methods));
		$log->debug('who2acl     ' . Dumper(\%who2acl));
	}

	$self->{backend2acl} = $backend2acl;
	$self->{method2acl} = $method2acl;
	$self->{methods} = \%methods;
	$self->{who2acl} = \%who2acl;
}

sub work {
	my ($self) = @_;
	if ($self->daemon) {
		daemonize();
	}

	local $SIG{TERM} = local $SIG{INT} = sub {
		$self->_shutdown(@_);
	};

	local $SIG{HUP} = sub {
		$self->log->info('trying to reload config');
		local $@;
		eval {
			$self->_load_config();
		};
		$self->log->error("config reload failed: $@") if $@;
	};

	$self->log->info('RPC::Switch starting work');
	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
	#my $reactor = Mojo::IOLoop->singleton->reactor;
	#$reactor->{running}++;
	#while($reactor->{running}) {
	#	$reactor->one_tick();
	#}
	$self->log->info('RPC::Switch done?');

	return 0;
}

sub rpc_ping {
	my ($self, $c, $r, $i, $rpccb) = @_;
	return 'pong?';
}


sub rpc_hello {
	my ($self, $con, $r, $args, $rpccb) = @_;
	#$self->log->debug('rpc_hello: '. Dumper($args));
	my $who = $args->{who} or die "no who?";
	my $method = $args->{method} or die "no method?";
	my $token = $args->{token} or die "no token?";

	$self->auth->authenticate($method, $con, $who, $token, sub {
		my ($res, $msg, $reqauth) = @_;
		if ($res) {
			$self->log->debug("hello from $who succeeded: method $method msg $msg");
			$con->who($who);
			$con->reqauth($reqauth);
			$con->state('auth');
			$rpccb->(JSON->true, "welcome to the rpcswitch $who!");
		} else {
			$self->log->debug("hello failed for $who: method $method msg $msg");
			$con->state(undef);
			# close the connecion after sending the response
			Mojo::IOLoop->next_tick(sub {
				$con->close;
			});
			$rpccb->(JSON->false, 'you\'re not welcome!');
		}
	});
}

sub _checkacl {
	my ($self, $acl, $who) = @_;
	
	$acl = [$acl] unless ref $acl;	

	#say "check if $who is in any of ('", join("', '", @$acl), "')";

	my $a = $self->{who2acl}->{$who} // { public => 1 };

	#say "$who is in ('", join("', '", keys %$a), "')";

	my @matches = grep { defined } @{$a}{@$acl};
	
	#print  'matches: ', Dumper(\@matches);

	return (scalar @matches > 0);
}

sub rpc_announce {
	my ($self, $con, $req, $i, $rpccb) = @_;
	$self->log->info("announce from $con->{who} ($con->{from})");
	my $method = $i->{method} or die 'method required';
	my $slots      = $i->{slots} // 1;
	my $workername = $i->{workername} // $con->workername // $con->who;
	my $filter     = $i->{filter};
	my $worker_id = $con->worker_id;
	unless ($worker_id) {
		$worker_id = ++$self->{worker_id};
		$con->worker_id($worker_id);
	}
	$self->log->debug("worker_id: $worker_id");

	# check if namespace.method matches a backend2acl (maybe using a namespace.* wildcard)
	# if not: fail
	# check if $client->who appears in that acl

	$method =~ /^([^.]+)\..*$/ 
		or die "no namespace in $method?";
	my $ns = $1;

	my $acl = $self->{backend2acl}->{$method}
		  // $self->{backend2acl}->{"$ns.*"}
		  // die "no backend acl for $method";

	my $who = $con->who;
	die "acl $acl does not allow announce of $method by $who"
		unless $self->_checkacl($acl, $con->who);

	my $wm = RPC::Switch::WorkerMethod->new(
		method => $method,
		connection => $con,
	);

	$con->workername($workername);
	#$client->worker_id($worker_id);
	die "already announced $wm" if $con->methods->{$method};
	$con->methods->{$method} = $wm;
	# note that this client is interested in this listenstring
	#push @{$self->listenstrings->{$method}}, $wa;

	if ($wm =~ /^(\w+\.)\*$/) {
		#wildcard announce
		die "uhm?";
	} else {
		if ($self->workermethods->{$method}) {
			push @{$self->workermethods->{$method}}, $wm
		} else {
			$self->workermethods->{$method} = [$wm];
		}
	}

	#$self->rpc->register($method, sub { $self->_call($method, @_) });

	# set up a ping timer to the client after the first succesfull announce
	unless ($con->tmr) {
		$con->{tmr} = Mojo::IOLoop->recurring( $con->ping, sub { $self->_ping($con) } );
	}
	$rpccb->(JSON->true, 'success');

	#$self->_task_ready($method, '{"poll":"first"}')
	#	if $self->pending->{$method};


	return
}

sub rpc_withdraw {
	my ($self, $con, $m, $i) = @_;
	my $method = $i->{method} or die 'method required';
	# find listenstring by method

	my $wm = $con->methods->{$method} or die "unknown method";
	# remove this action from the clients action list
	delete $con->methods->{$method};

	# now remove this workeraction from the listenstring workeraction list
	my $l = $self->workermethods->{$method};
	my @idx = grep { refaddr $$l[$_] == refaddr $wm } 0..$#$l;
	splice @$l, $_, 1 for @idx;
	delete $self->workermethods->{$method} unless @$l;

	if (not $con->methods and $con->tmr) {
		# cleanup ping timer if client has no more actions
		$self->log->debug("remove tmr $con->{tmr}");
		Mojo::IOLoop->remove($con->tmr);
		delete $con->{tmr};
	}

	return 1;
}

sub _ping {
	my ($self, $con) = @_;
	my $tmr;
	Mojo::IOLoop->delay->steps(sub {
		my $d = shift;
		my $e = $d->begin;
		$tmr = Mojo::IOLoop->timer(10 => sub { $e->(@_, 'timeout') } );
		$con->call('rpcswitch.ping', {}, sub { $e->($con, @_) });
	},
	sub {
		my ($d, $e, $r) = @_;
		#print  'got ', Dumper(\@_);
		if ($e and $e eq 'timeout') {
			$self->log->info('uhoh, ping timeout for ' . $con->who);
			Mojo::IOLoop->remove($con->id); # disconnect
		} else {
			if ($e) {
				$self->log->debug("'got $e->{message} ($e->{code}) from $con->{who}");
				return;
			}
			$self->log->debug('got ' . $r . ' from ' . $con->who . ' : ping(' . $con->worker_id . ')');
			Mojo::IOLoop->remove($tmr);
		}
	});
}


# not a method!
sub _rot {
	my $l = shift;
	push @$l, shift @$l;
}

sub _shutdown {
	my($self, $sig) = @_;
	$self->log->info("caught sig$sig, shutting down");

	#for my $wms (values %{$self->methods}) {
	#	for my $wa (@$was) {
	#		$self->log->debug("withdrawing '$wa->{method}' for '$wa->{client}->{workername}'");
	#	}
	#}

	Mojo::IOLoop->stop;
}


sub register {
	my ($self, $name, $cb, %opts) = @_;
	my %defaults = ( 
		by_name => 1,
		non_blocking => 0,
		notification => 0,
		state => undef,
	);
	croak 'no self?' unless $self;
	croak 'no callback?' unless ref $cb eq 'CODE';
	%opts = (%defaults, %opts);
	croak 'a non_blocking notification is not sensible'
		if $opts{non_blocking} and $opts{notification};
	croak "internal methods need to start with rpcswitch." unless $name =~ /^rpcswitch\./;
	croak "procedure $name already registered" if $self->{internal}->{$name};
	$self->{internal}->{$name} = { 
		name => $name,
		cb => $cb,
		by_name => $opts{by_name},
		non_blocking => $opts{non_blocking},
		notification => $opts{notification},
		state => $opts{state},
	};
}


sub _handle_internal_request {
	my ($self, $c, $r) = @_;
	my $m = $self->{internal}->{$r->{method}};
	#return $self->_catchall($c, $r) unless $m;
	my $id = $r->{id};
	return $self->_error($c, $id, ERR_METHOD, 'Method not found.') unless $m;
	#$m = $$m[rand @$m] if ref $m eq 'ARRAY';
	#$self->log->debug('	m: ' . Dumper($m));
	return $self->_error($c, $id, ERR_NOTNOT, 'Method is not a notification.') if !$id and !$m->{notification};

	return $self->_error($c, $id, ERR_REQ, 'Invalid Request: params should be array or object.')
		if ref $r->{params} ne 'ARRAY' and ref $r->{params} ne 'HASH';

	return $self->_error($c, $id, ERR_PARAMS, 'This method expects '.($m->{by_name} ? 'named' : 'positional').' params.')
		if ref $r->{params} ne ($m->{by_name} ? 'HASH' : 'ARRAY');
	
	return $self->_error($c, $id, ERR_BADSTATE, 'This method requires connection state ' . ($m->{state} // 'undef'))
		if $m->{state} and not ($c->state and $m->{state} eq $c->state);

	my $cb;
	$cb = sub { $self->_result($c, $id, \@_) if $id; } if $m->{non_blocking};

	local $@;
	my @ret = eval { $m->{cb}->($c, $r, $r->{params}, $cb)};
	return $self->_error($c, $id, ERR_ERR, "Method threw error: $@") if $@;
	#$self->log->debug('method returned: '. Dumper(\@ret));
	
	return $self->_result($c, $id, \@ret) if !$cb and $id;
	return;
}

sub _handle_request {
	my ($self, $c, $request) = @_;
	$self->log->debug('    in handle_request');
	my $method = $request->{method} or die 'huh?';
	my $id = $request->{id};

	#print  'methods: ', Dumper($self->methods);

	my $backend = $self->methods->{$method};
	#return $self->_error($c, $id, ERR_METHOD, 'Method not found.') unless $backend;
	return $self->_handle_internal_request($c, $request) unless $backend;

	$self->log->debug("rpc_catchall for $method");

	# auth only beyond this point
	return $self->_error($c, $id, ERR_BADSTATE, 'This method requires an authenticated connection')
		unless ($c->state and 'auth' eq $c->state);

	# check if $c->who is in the method2acl for this method?
	$method =~ /^([^.]+)\..*$/ 
		or return $self->_error($c, $id, ERR_NONS, "no namespace in $method?");
	my $ns = $1;

	my $acl = $self->{method2acl}->{$method}
		  // $self->{method2acl}->{"$ns.*"}
		  // return $self->_error($c, $id, ERR_NOACL, "no method acl for $method");

	my $who = $c->who;
	return $self->_error($c, $id, ERR_NOTAL, "acl $acl does not allow method $method for $who")
		unless $self->_checkacl($acl, $who);

	#print  'workermethods: ', Dumper($self->workermethods);

	# todo: implement filtering here
	my $l = $self->workermethods->{$backend};

	#print  'l: ', Dumper($l);

	return return $self->_error($c, $id, ERR_NOWORKER, "No worker avaiable for $backend.") unless $l and @$l;

	_rot($l); # rotate workermethord
	#sort $l by refcount?
	my $wm = (sort { $a->refcount <=> $b->refcount} @$l)[0];
	return $self->_error($c, $id, ERR_INTERNAL, 'Internal error.') unless $wm;
	#my $wm = @$l[0] or return $self->_error($c, $id, ERR_INTERNAL, 'Internal error.');

	my $wcon = $wm->connection;
	$self->log->debug('forwarding '. $method .' to '. $wcon->worker_id .' for '. $backend);

	# find or create channel
	my $vci = md5_base64(refaddr($c).':'.refaddr($wcon)); # should be unique for this instance?
	my $channel;
	unless ($channel = $c->channels->{$vci}) {
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
	#++$channel->{refcount} if $id;
	$channel->reqs->{$id} = 1 if $id;
	$self->log->debug("refcount $channel " . scalar keys %{$channel->reqs});

	#if ($backend =~ /^(\w+\.)\*$/) {
	#	$backend = $1;
	#	$method =~ /^\w+\.(.*)$/);
	#	$backend .= $1;
	#}

	# rewrite request
	my $workerrequest = {
		jsonrpc => '2.0',
		rpcswitch => {
			vcookie => 'eatme', # channel information version
			vci => $vci,
			who => $c->who,
		},
		method => $backend,
		params => $request->{params},
		id  => $id,
	};
	
	# forward request to worker
	$wcon->_write(encode_json($workerrequest));
	return; # exlplicit empty return
}

sub _handle_channel {
	my ($self, $c, $r) = @_;
	$self->log->debug('    in handle_channel');
	my $rpcswitch = $r->{rpcswitch};
	my $id = $r->{id};
	
	# fixme: error on a response?
	unless ($rpcswitch->{vcookie} and $rpcswitch->{vcookie} eq 'eatme'
	       and $rpcswitch->{vci}) {
		return $self->_error($c, $id, ERR_BADCHAN, 'Invalid channel information') if $r->{method};
		$self->log->info("invalid channel information from $c"); # better error message?
		return;
	}

	#print 'rpcswitch: ', Dumper($rpcswitch);
	#print 'channels; ', Dumper($self->channels);
	my $chan = $c->channels->{$rpcswitch->{vci}};
	my $con;
	my $dir;
	if ($chan) {
		if (refaddr($c) == refaddr($chan->worker)) {
			# worker to client
			$con = $chan->client;
			$dir = -1;
		} elsif (refaddr($c) == refaddr($chan->client)) {
			# client to worker
			$con = $chan->worker;
			$dir = 1;
		} else {
			$chan = undef;
		}
	}		
	unless ($chan) {
		return $self->_error($c, $id, ERR_NOCHAN, 'No such channel.') if $r->{method};
		$self->log->info("invalid channel from $c");
		return;
	}		
	#$chan->{refcount} += ($r->{method}) ? 1 : -1 if $id;
	#$self->log->debug("refcount $chan $chan->{refcount}");
	if ($id) {
		if ($r->{method}) {
			$chan->reqs->{$id} = $dir;
		} else {
			delete $chan->reqs->{$id};
		}
	}
	$self->log->debug("refcount $chan " . scalar keys %{$chan->reqs});
	#print Dumper($chan->reqs);
	# forward request
	$con->_write(encode_json($r));
	return;
}


sub _error {
	my ($self, $c, $id, $code, $message, $data) = @_;
	return $c->_error($id, $code, $message, $data);
}

sub _result {
	my ($self, $c, $id, $result) = @_;
	$result = $$result[0] if scalar(@$result) == 1;
	#$self->log->debug('_result: ' . Dumper($result));
	$c->_write(encode_json({
		jsonrpc	    => '2.0',
		id	    => $id,
		result	    => $result,
	}));
	return;
}

sub _disconnect {
	my ($self, $client) = @_;
	$self->log->info('oh my.... ' . ($client->who // 'somebody')
				. ' (' . $client->from . ') disonnected..');

	return unless $client->who;

	for my $m (keys %{$client->methods}) {
		$self->log->debug("withdrawing $m");
		# hack.. fake a rpcswitch.withdraw request
		$self->rpc_withdraw($client, {method => 'withdraw'}, {method => $m});
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
		#delete $self->channels->{$vci};
		$c->delete();
	}
}


1;

__END__


sub _call {
	my ($self, $con, $method, $args, $rpccb) = @_;
	
	die '_call: no method?' unless $method;

	my $l = $self->listenstrings->{$method};
	return unless $l; # should not happen? maybe unlisten here?

	_rot($l); # rotate listenstrings list (list of workeractions)
	my $wa;
	for (@$l) { # now find a worker with a free slot
		my $worker_id = $_->client->worker_id;
		$self->log->debug('worker ' . $worker_id . ' has ' . $_->used . ' of ' . $_->slots . ' used');
		if ($_->used < $_->slots) {
			$wa = $_;
			last;
		}
	}

	unless ($wa) {
		$self->log->debug("no free slots for $method!?");
		$self->pending->{$method} = 1;
		# we'll do a poll when a worker becomes available
		# and the maestro will bother us again later anyways
		return;
	}

	$self->log->debug('sending task ready to worker ' . $wa->client->worker_id . ' for ' . $wa->method);

	print Dumper($args);
	$wa->client->con->call($method, $args);
}

sub rpc_task_done {
	#my ($self, $task, $outargs) = @_;
	my ($self, $con, $i, $rpccb) = @_;
	my $client = $con->owner;
	my $cookie = $i->{cookie} or die 'no cookie?';
	my $outargs = $i->{outargs} or die 'no outargs?';

	my $task = delete $self->{tasks}->{$cookie};
	return unless $task; # really?	
	Mojo::IOLoop->remove($task->tmr) if $task->tmr;
	$task->workeraction->{used}--; # done..

	local $@;
	eval {
		$outargs = encode_json( $outargs );
	};
	$outargs = encode_json({'error' => 'cannot json encode outargs: ' . $@}) if $@;
	#$self->log->debug("outargs $outargs");
	#eval {
	#	$self->pg->db->dollar_only->query(q[select task_done($1, $2)], $cookie, $outargs, sub { 1; } );
	#};
	$self->log->debug("task_done got $@") if $@;
	$self->log->debug("worker '$client->{workername}' done with action '$task->{method}' for job $task->{job_id}"
		." slots used $task->{workeraction}->{used} outargs '$outargs'");

	Mojo::IOLoop->delay->steps(
		sub {
			my $d = shift;
			$self->pg->db->dollar_only->query(
				q[select task_done($1, $2)],
				$cookie, $outargs, $d->begin
			);
		},
		sub {
			my ($d, $err, $res) = @_;
			if ($err) {
				$self->log->error("get_task threw $err");
				return;
			}
			$self->log->debug("task_done_callback!");
			if ($self->pending->{$task->workeraction->listenstring}) {
				$self->log->debug("calling _task_readty from task_done_callback!");
				$self->_task_ready(
					$task->workeraction->listenstring,
					'{"poll":"please"}'
				);
			}
		},
	);
	return;
}

sub _task_timeout {
	my ($self, $cookie) = @_;
	my $task = delete $self->{tasks}->{$cookie};
	return unless $task; # really?
	$task->workeraction->{used}++; # done..

	my $outargs = encode_json({'error' => 'timeout after ' . $self->timeout . ' seconds'});
	eval {
		$self->pg->db->dollar_only->query(q[select task_done($1, $2)], $cookie, $outargs, sub { 1; } );
	};
	$self->log->debug("task_done got $@") if $@;
	$self->log->debug("timeout for action $task->{method} for job $task->{job_id}");
}


sub rpc_create_job {
	my ($self, $con, $m, $i, $rpccb) = @_;
	my $client = $con->owner;
	#$self->log->debug('create_job: ' . Dumper(\@_));
	my $wfname = $i->{wfname} or die 'no workflowname?';
	my $inargs = $i->{inargs} // '{}';
	my $vtag = $i->{vtag};
	my $timeout = $i->{timeout} // 60;
	my $impersonate = $client->who;
	my $env;
	if ($client->reqauth) {
		my ($res, $log, $authscope) = $client->reqauth->request_authentication($client, $i->{reqauth});
		unless ($res) {
			$rpccb->(undef, $log);
			return;
		}
		$env = decode_utf8(encode_json({authscope => $authscope}));
	}
	my $cb = sub {
		my ($job_id, $outargs) = @_;
		$con->notify('job_done', {job_id => $job_id, outargs => $outargs});
	};

	die  'inargs should be a hashref' unless ref $inargs eq 'HASH';
	$inargs = decode_utf8(encode_json($inargs));

	$self->log->debug("calling $wfname with '$inargs'" . (($vtag) ? " (vtag $vtag)" : ''));

	# create_job throws an error when:
	# - wfname does not exist
	# - inargs not valid
	Mojo::IOLoop->delay->steps(
		sub {
			my $d = shift;
			#($job_id, $listenstring) = @{
			$self->pg->db->dollar_only->query(
				q[select * from create_job(wfname := $1, args := $2, tag := $3, impersonate := $4, env := $5)],
				$wfname,
				$inargs,
				$vtag,
				$impersonate,
				$env,
				$d->begin
			);
		},
		sub {
			my ($d, $err, $res) = @_;

			if ($err) {
				$rpccb->(undef, $err);
				return;
			}
			my ($job_id, $listenstring) = @{$res->array};
			unless ($job_id) {
				$rpccb->(undef, "no result from call to create_job");
				return;
			}

			# report back to our caller immediately
			# this prevents the job_done notification overtaking the 
			# 'job created' result...
			$self->log->debug("created job_id $job_id listenstring $listenstring");
			$rpccb->($job_id, undef);

			my $job = RPC::Switch::Job->new(
				cb => $cb,
				job_id => $job_id,
				inargs => $inargs,
				listenstring => $listenstring,
				vtag => $vtag,
				wfname => $wfname,
			);

			#$self->pg->pubsub->listen($listenstring, sub {
			# fixme: 1 central listen?
			my $lcb = $self->pg->pubsub->listen('job:finished', sub {
				my ($pubsub, $payload) = @_;
				return unless $job_id == $payload;
				local $@;
				eval { $self->_poll_done($job); };
				$self->log->debug("pubsub cb $@") if $@;
			});

			my $tmr = Mojo::IOLoop->timer($timeout => sub {
				# request failed, cleanup
				#$self->pg->pubsub->unlisten($listenstring);
				$self->pg->pubsub->unlisten('job:finished' => $lcb);
				# the cb might fail if the connection is gone..
				eval { &$cb($job_id, {'error' => 'timeout'}); };
				$job->destroy;
			});

			$job->update(tmr => $tmr, lcb => $lcb);

			# do one poll first..
			$self->_poll_done($job);
		}
	)->catch(sub {
		my ($delay, $err) = @_;
		$rpccb->(undef, $err);
	});
}

