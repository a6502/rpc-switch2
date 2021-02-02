package RPC::Switch::Processor;

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
use Fcntl qw(:DEFAULT);
use File::Basename;
use File::Temp qw(tempfile);
use FindBin qw($RealBin $RealScript);
use List::Util qw(shuffle);
use POSIX ();
use Scalar::Util qw(blessed refaddr weaken);

# more cpan
use JSON::MaybeXS;
use Ref::Util qw(is_arrayref is_coderef is_hashref);

# RPC Switch aka us
use RPC::Switch::ReqAuth;
use RPC::Switch::Shared;

use constant DEBUG => $ENV{RPC_SWITCH_PROCESSOR_DEBUG} || 0;

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
	ERR_TOOBIG   => -32011, # Req/Resp object too big
	ERR_REQAUTH_REQUIRED => -32012, # Reqauth not present
	ERR_REQAUTH_FAILED   => -32013, # Reqauth failed
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

# lotsa (shared) globals
our (
	%bstat,
	$child,
	$chunks,
	$concount,
	%cons,
	$decoder,
	$debug,
	$encoder,
	%internal,
	$ioloop,
	$log,
	%mstat,
	@mq,
	$numproc,
	$tmpdir,
);

sub _init {
	# register internal methods
	_register('rpcswitch.announce', \&rpc_announce, non_blocking => 1, state => 'auth');
	_register('rpcswitch.get_clients', \&rpc_get_clients, state => 'auth');
	_register('rpcswitch.get_method_details', \&rpc_get_method_details, state => 'auth');
	_register('rpcswitch.get_methods', \&rpc_get_methods, state => 'auth');
	_register('rpcswitch.get_stats', \&rpc_get_stats, state => 'auth');
	_register('rpcswitch.get_workers', \&rpc_get_workers, state => 'auth');
	_register('rpcswitch.hello', \&rpc_hello, non_blocking => 1);
	_register('rpcswitch.ping', \&rpc_ping);
	_register('rpcswitch.withdraw', \&rpc_withdraw, state => 'auth');
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
	croak "method $name already registered" if $internal{$name};
	$internal{$name} = {
		name => $name,
		cb => $cb,
		by_name => $opts{by_name},
		non_blocking => $opts{non_blocking},
		notification => $opts{notification},
		raw => $opts{raw},
		state => $opts{state},
	};
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

	RPC::Switch::Auth::authenticate($method, $con, $who, $token, sub {
		my ($res, $msg) = @_;
		if ($res) {
			$log->info("hello from $who succeeded: method $method msg $msg");
			upd('cons', $con->cid, {state => 'auth', who => $who});
			$con->who($who);
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

	my $who = $con->who;
	# fixme: acl for this?
	my %res;

	my $cons = all('cons');
	while (my ($cid, $c) = each %$cons ) {
		#print "cid $cid: ", Dumper($c);
		$res{$c->{from}} = {
			localname => $c->{localname},
			who => $c->{who},
			($c->{workername} ? (workername => $c->{workername}) : ()),
			($c->{workermethods} ? (methods => $c->{workermethods}) : ()),
		}
	}

	# follow the rpc-switch calling conventions here
	return (RES_OK, \%res);
}

sub rpc_get_method_details {
	my ($con, $r, $i, $cb) = @_;
	#my $con = sel('cons', $cid);

	my $who = $con->{who};

	my $method = $i->{method} or die 'method required';

	my $md = sel('methods', $method) #$methods->{$method}
		or die "method $method not found";

	$method =~ /^([^.]+)\..*$/
		or die "no namespace in $method?";
	my $ns = $1;

	my $acl = sel('method2acl', $method)
		  // sel('method2acl', "$ns.*")
		  // die "no method acl for $method";

	die "acl $acl does not allow calling of $method by $who"
		unless _checkacl($acl, $who);

	$md = { %$md }; # shallow copy to clobber

	# now find a backend
	my $backend = $md->{b};

	my $doc = sel('doc', $backend);
	$md->{doc} = $doc if defined $doc;

	#my $l = $workermethods->{$backend};
	my $l = sel('wm', $backend);
	#print '$l ', Dumper($l);

	$md->{msg} = 'no backend worker available' unless $l;

	# follow the rpc-switch calling conventions here
	return (RES_OK, $md);
}

sub rpc_get_methods {
	my ($con, $r, $i) = @_;
	#my $con = sel('cons', $cid);

	my $who = $con->{who};
	my @m;

	my $methods = all('methods');
	for my $method ( keys %$methods ) {
		$method =~ /^([^.]+)\..*$/
			or next;
		my $ns = $1;
		my $acl = sel('method2acl', $method)
			  // sel('method2acl', "$ns.*")
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

	local $/ = undef;

	my %stats;
	for my $c (1..$numproc) {
		open(my $fh, "$tmpdir/st/$c.json") or next;
		my $s = decode_json(<$fh>);
		close($fh);
		for (qw(chunks clients connections workers)) {
			$stats{$_} += $s->{$_};
		}
		my $m = $s->{methods} // next;
		my $tm = $stats{methods} //= {};
		for (keys %$m) {
			$tm->{$_} += $m->{$_};
		}
	}	

	# follow the rpc-switch calling conventions here
	return (RES_OK, \%stats);
}

sub rpc_get_workers {
	my ($con, $r, $i) = @_;

	#my $who = $con->who;
	# fixme: acl for this?

	#print 'workermethods: ', Dumper($workermethods);
	my %workers;

	my $wms = all('wm');
	#print 'wms: ', Dumper($wms);
	for my $m ( keys %$wms ) {
		my $l = allforkey('wm', $m);
		for my $wm (@$l) {
			#my $con = sel('cons', $wm->{connection});
			#push @{$workers{$con->{workername}}}, $wm->{method};
			push @{$workers{$wm->{cid}}}, $wm->{method};
		}
		

		#if (is_arrayref($l) and @$l) {
		#	#print 'l : ', Dumper($l);
		#	for my $wm (@$l) {
		#		my $con = sel('cons', $wm->{connection});
		#		push @{$workers{$con->{workername}}}, $wm->{method};
		#	}
		#} elsif (is_hashref($l)) {
		#	# filtering
		#	keys %$l; # reset each
		#	while (my ($f, $wl) = each %$l) {
		#		for my $wm (@$wl) {
		#			push @{$workers{$wm->connection->workername}}, [$wm->method, $f];
		#		}
		#	}
		#}
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

	#my $a = $who2acl->{$who} // { public => 1 };
	my $a = sel('who2acl', $who) // { public => 1 };
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
	#my $a = $who2acl->{$_[1]} // { public => 1 };
	my $a = sel('who2acl', $_[1], $_[2]) // { public => 1 };
	return scalar grep(defined, @{$a}{@{$_[0]}}) if ref $_[0];
	return $a->{$_[0]};
}

sub rpc_announce {
	my ($con, $req, $i, $rpccb) = @_;
	my $method = $i->{method} or die 'method required';
	#my $con = sel('cons', $cid);
	my $who = $con->{who};
	$log->info("announce of $method from $who ($con->{from})");
	my $workername = $i->{workername} // $con->{workername} // $con->{who};
	my $filter     = $i->{filter};
	#my $worker_id = $con->worker_id;
	#unless ($worker_id) {
	#	# it's a new worker: assign id
	#	$worker_id = ++$last_worker_id;
	#	$con->worker_id($worker_id);
	#	$workers++; # and count
	#}
	#$log->debug("worker_id: $worker_id");

	# check if namespace.method matches a backend2acl (maybe using a namespace.* wildcard)
	# if not: fail
	# check if $client->who appears in that acl

	$method =~ /^([^.]+)\..*$/ 
		or die "no namespace in $method?\n";
	my $ns = $1;

	my $acl = sel('backend2acl', $method)
		  // sel('backend2acl', "$ns.*")
		  // die "no backend acl for $method\n";

	unless (_checkacl($acl, $who)) {
		$acl = '[' . join(',', @$acl) . ']' if is_arrayref($acl);
		die "acl $acl does not allow announce of $method by $who\n"
	}
	# now check for filtering

	my $filterkey = sel('backendfilter', $method)
			// sel('backendfilter', "$ns.*");

	my $filtervalue;

	if ($filterkey) {
		$log->debug("looking for filterkey $filterkey");
		if ($filter) {
			die "filter should be a json object" unless is_hashref($filter);
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

	# latest announce determines doc
	ins('doc', $method, $i->{doc}) if $i->{doc};

	my %wmh = (
		method => $method,
		child => $child,
		cid => $con->cid,
		# doc can be too large to fit in a dupsort value
		#($i->{doc} ? (doc => $i->{doc}) : ()),
		($filtervalue ? (
			filtervalue => $filtervalue
		) : ()),
	);
	#$log->debug('wmh: ', Dumper(\%wmh));

	#$sql->db->query("insert into wm (method, cid, val) values (?,?,?)", $method, $cid, encode_json($foo));
	my $tmp = $method;
	$tmp .= "\n$filtervalue" if $filtervalue;
	ins('wm', $tmp, \%wmh);

	#my $wm = RPC::Switch::WorkerMethod->new(%wmh);

	#die "already announced $wm" if $con->methods->{$method};
	#$con->methods->{$method} = $wm;

	$con->workername($workername) unless $con->workername;
	my $wms = $con->{workermethods} //= [];
	push @$wms, $tmp;
	# fixme: do we need this?
	upd('cons', $con->cid, {workermethods => $wms, workername => $workername} );

	#if ($filterkey) {
	#	push @{$wms->{$method}->{$filtervalue}}, $wm
	#} else {
	#	push @{$wms->{$method}}, $wm;
	#}

	# set up a ping timer to the client after the first succesfull announce
	unless ($con->tmr) {
		$log->debug("setting up ping timer for $con ($workername)");
		$con->{tmr} = $ioloop->recurring( $con->ping, sub { _ping($con) } );
	}

	$rpccb->(JSON->true, { msg => 'success', worker_id => $con->cid });

	# fixme: make non-async?
	return;
}

sub rpc_withdraw {
	my ($con, $m, $i) = @_;
	my $method = $i->{method} or die 'method required';
	#my $con = sel('cons', $cid);
	#my $who = $con->{who};
	#$log->info("withdraw of $method from $who ($con->{from})");

	my $cid = $con->cid;

	$log->info("withdraw of $method by " . ($cid // 'unknown') 
				. ' (' . ($con->{who} // 'somebody')
				. ' ) from ' . ($con->{from} // 'somewhere') . ') disonnected..');

	# first try to update the connection workermethod list
	my $wms = $con->{workermethods};
	die 'nothing announced?'  unless is_arrayref($wms);
	
	# todo: filtering?
	my @m = grep(@$wms[$_] eq $method, 0..$#$wms);
	die "method $method was not announced" unless @m;
	splice @$wms, $_, 1 for @m;
	#upd('cons', $con->cid, {workermethods => $wms});

	unless (@$wms) {
		# cleanup ping timer if connection/client has no more methods
		$log->debug("remove tmr $con->{tmr}");
		$ioloop->remove($con->{tmr});
	}
	
	# we need the json value here because we need the exact string value to delete
	my $foo = allforkey('wm', $method, 'json');
	next unless $foo;
	#print "wm $wm: ", Dumper($foo);
	my @foo = grep { my $bar = decode_json($_); $bar->{cid} eq $cid } @$foo;
	del('wm', $method, $foo[0]) if @foo;

	return 1;
}

sub _ping {
	my ($con) = @_;
	my $tmr;
	$log->debug("_ping for '$con->{cid}'");
	#print 'cons_ ', Dumper(all('cons'));
	#my $c = sel('cons', $con->cid);
	$ioloop->delay(sub {
		my $d = shift;
		my $e = $d->begin;
		$tmr = $ioloop->timer(10 => sub { $e->(@_, 'timeout') } );
		$con->call('rpcswitch.ping', {}, sub { $e->($con, @_) });
	},
	sub {
		my ($d, $e, $r) = @_;
		#print  '_ping got ', Dumper(\@_);
		if ($e and $e eq 'timeout') {
			$log->info('uhoh, ping timeout for ' . $con->{who});
			#$ioloop->remove($con->id); # disconnect
			$con->close();
		} else {
			$ioloop->remove($tmr);
			if ($e) {
				$log->debug("'got $e->{message} ($e->{code}) from $con->{who}");
				return;
			}
			$log->debug("got $r from $con->{who} : ping( $con->{cid} )");
		}
	})->catch(sub {
		my ($err) = @_;
		$log->error("Something went wrong in _ping: $err");
	});
}

sub _send_to_mq {
	use bytes;
	my ($q, $res) = @_;
	my $eres = $encoder->encode($res);
	if (length($eres) > 4000) { # fixme: do not hardcode
		my $nres;
		local $@;
		eval {
		my ($fh, $filename) = tempfile( DIR => "$tmpdir/xl" );
		$log->debug("writing to tempfile $filename");
		(syswrite($fh, $eres) // -1) == length $eres
			or die qq{Can't write to file "$filename": $!};
		close $fh;
		$nres = {xl => $filename};
		$eres = $encoder->encode($nres);
		};
		$log->error("error: ", $@) if $@;
		$log->debug("send_to_mq: " . Dumper($nres)) if $debug;
		$q->send($eres);
	} else {
		$log->debug("send_to_mq: " . Dumper($res)) if $debug;
		$q->send($eres);
	}
}

# receive in processsor
sub _handle_q_request {
	local $@;
	#$debug = 0;

	#$SIG{TERM} = $SIG{INT} = sub {
	#	my ($sig) = @_;
	#	$log->info("$$ caught sig$sig, shutting down");
	#	DB::finish_profile();
	#	exit(0);
	#};

	my ($resq, $msg, $prio) = @_;

	#$log->debug('__handle_q_request!') if $debug;
	eval {
		$msg = $decoder->decode($msg)
	};
	if ($@) {
		$log->error("msg decode failed: $@");
		return;
	}
	$log->debug('_handle_q_request: ' . Dumper($msg)) if $debug;
	LAZINESS:
	if ($msg->{chunk}) {
		eval {
			_forward_channel($msg);
		};
		if ($@) {
			$log->error("_forward_channel: $@");
			# todo: send error back?
			#_error($cid, undef, ERR_INTERNAL, $@);
			return;
		}
	} elsif (my $filename = $msg->{xl}) {
		$log->debug("xl read $filename");
		open my $fh, '<', $filename or die qq{Can't open file "$filename": $!};
		my $ret = my $buffer = my $realmsg = '';
		while ($ret = sysread($fh, $buffer, 131072, 0)) { $realmsg .= $buffer }
		die qq{Can't read from file "$filename": $!} unless defined $ret;
		close $fh;
		unlink $filename or die "cannot unlink $filename: $!";
		eval { $msg = $decoder->decode($realmsg) };
		if ($@) {
			$log->error("msg decode failed: $@");
			return;
		}
		$log->debug('xl request: ', Dumper($msg)) if $debug;
		goto LAZINESS;
	} elsif ($msg->{channel_gone}) {
		_channel_gone($msg);
	} else {
		$log->error("huh? strange msg: " .  Dumper($msg));
	}
	#mstat("$$");
}

# msg from mq to client con
sub _forward_channel {
	my ($msg) = @_;
	my ($json, $tocid, $fromcid, $fromchild, $id, $dir, $vci) = 
		@$msg{qw(chunk tocid fromcid fromchild id dir vci)};
	#print "$$ Channels: ", Dumper(\%RPC::Switch::cons);

	my $tocon = $cons{$tocid} or die "no such connection $tocid";

	unless ($tocon->{ns}) {
		#print 'tocon: ', Dumper($tocon);
		die 'uh?';
	}

	my $fromcon = $mq[$fromchild] or die "mq for $fromchild not found!?";

	my $channel;
	# fixme: for return msgs channel should exist?
	unless ($channel = $tocon->{channels}->{$vci}) {
		if ($dir < 0) {
			$log->info("_forward_channel: channel $vci not found for dir < 0");
			return;
		}
		$channel = RPC::Switch::Channel->new(
			ccid => $fromcid,
			client => $fromcon, #$cid,
			#cflag => 1, # mq
			vci => $vci,
			worker => $tocon, #$wcid,
			wcid => $tocid,
			#refcount => 0,
			reqs => {},
		);
		$tocon->{channels}->{$vci} =
			#$fromcon->{channels}->{$vci} =
				$channel;
	}
	#$tocon->{refcount} += $refmod;
	if ($id and $dir < 0) {
		delete $channel->{reqs}->{$id};
		#if (delete $channel->{reqs}->{$id}) {
		#	say "_forward_channel: deleted $id";
		#} else {
		#	say "_forward_channel: failed to delete $id";
		#}
		$log->debug("refcount channel $channel " . scalar keys %{$channel->{reqs}}) if $debug;
		#$sql->db->query("insert into reqs values (?,?)", $vci, $id);
		#ins('reqs', $vci, $id);
	}

	$log->debug('    writing: ' . decode_utf8($json)) if $debug;
	$tocon->{ns}->write($json);
	return;
}

sub _handle_internal_request {
	my ($con, $r) = @_;
	my $method = $r->{method};
	my $m = $internal{$r->{method}}; # sel('internal', $method); # $internal->{$r->{method}};
	my $id = $r->{id};
	return _error($con, $id, ERR_METHOD, "Method '$method' not found.") unless $m;
	my $params = $r->{params};

	#$log->debug('	m: ' . Dumper($m));
	return _error($con, $id, ERR_NOTNOT, "Method '$method' is not a notification.") if !$id and !$m->{notification};

	return _error($con, $id, ERR_REQ, 'Invalid Request: params should be array or object.')
		unless is_arrayref($params) or is_hashref($params);

	return _error($con, $id, ERR_PARAMS, "Method '$method' expects ".($m->{by_name} ? 'named' : 'positional').' params.')
		if ref $params ne ($m->{by_name} ? 'HASH' : 'ARRAY');
	
	return _error($con, $id, ERR_BADSTATE, "Method '$method' requires connection state " . ($m->{state} // 'undef'))
		if $m->{state} and not ($con->{state} and $m->{state} eq $con->{state});

	$mstat{$method}++;
	my $reqcb = $m->{cb};
	if ($m->{raw}) {
		my $cb;
		$cb = sub { $con->_write(encode_json($_[0])) if $id } if $m->{non_blocking};

		local $@;
		#my @ret = eval { $m->{cb}->($c, $jsonr, $r, $cb)};
		my @ret = eval { $reqcb->($con, $r, $cb)};
		return _error($con, $id, ERR_ERR, "Method threw error: $@") if $@;
		#say STDERR 'method returned: ', Dumper(\@ret);

		$con->_write(encode_json($ret[0])) if !$cb and $id;
		return
	}

	my $cb;
	$cb = sub { _result($con, $id, \@_) if $id; } if $m->{non_blocking};

	local $@;
	my @ret = eval { $reqcb->($con, $r, $params, $cb) };
	return _error($con, $id, ERR_ERR, "Method threw error: $@") if $@;
	$log->debug('method returned: '. Dumper(\@ret));
	
	return _result($con, $id, \@ret) if !$cb and $id;
	return;
}

sub _handle_request {
	my ($con, $request) = @_;
	#$log->debug('    in handle_request: ' . Dumper($request)) if $debug;
	my $id = $request->{id};
	my $method = $request->{method} or
		return _error($con, $id, ERR_METHOD, 'Method "" does not exist');

	my $txn = txn();
	my $md = sel('methods', $method, $txn);
	unless ($md) {
		$txn->commit();
		goto &_handle_internal_request;
		#return _handle_internal_request(@_);
	}
	$log->debug("rpc_catchall for $method") if $debug;

	#my $con = sel('cons', $cid);
	# auth only beyond this point
	unless ($con->{state} and 'auth' eq $con->{state}) {
		$txn->commit();
		return _error($con, $id, ERR_BADSTATE, 'This method requires an authenticated connection');
	}

	# check if $c->who is in the method2acl for this method?
	my ($ns) = split /\./, $method, 2;
	unless ($ns) {
		$txn->commit();
		return _error($con, $id, ERR_NONS, "no namespace in $method?");
	}

	#my $acl = sel('method2acl', $method, $txn)
	#	  // sel('method2acl', "$ns.*", $txn);
	#unless ($acl) {
	#	$txn->commit();
	#	return _error($con, $id, ERR_NOACL, "no method acl for $method");
	#}

	my $acl = $md->{_a};
	my $who = $con->{who};
	
	unless (_checkacl($acl, $who, $txn)) {
		$txn->commit();
		return _error($con, $id, ERR_NOTAL, "acl $acl does not allow method $method for $who")
	}

	$txn->commit(); #meh
	
	if (my $ras = $md->{r}) {
		my $reqauth;
		$reqauth = $request->{rpcswitch}->{reqauth} if is_hashref($request->{rpcswitch});
		if (is_hashref($reqauth)) {
			my ($r, $e) = RPC::Switch::ReqAuth::authenticate_request($ras, $reqauth, sub {
				my ($r, $e) = @_;

				unless ($r) {
					$e //= '';
					$e = "request authentication failed for method $method: $e";
					$log->error($e);
					return _error($con, $id, ERR_REQAUTH_FAILED, $e);
				}

				$request->{rpcswitch}->{reqauth} = $r;

				_do_dispatch($con, $request, $md);
			});

			return unless defined $r; # the callback will resume processing..
			unless ($r) {
				$e //= '';
				$e = "request authentication failed for method $method: $e";
				$log->error($e);
				return _error($con, $id, ERR_REQAUTH_FAILED, $e);
			}
			$request->{rpcswitch}->{reqauth} = $r;
		} else {
			return _error($con, $id, ERR_REQAUTH_REQUIRED,
				"request authentication required for method $method but not present")
					unless $md->{o};
		}
	} else {
		delete $request->{rpcswitch}->{reqauth}
			if is_hashref($request->{rpcswitch}); # do not leak information
	}

	# we expect request_authentication to be very common.  Inlining
	# do_dispatch here would not gain much and duplicate code
	#return _do_dispatch($con, $request, $md);
	# but we can use the tail recursion hack:
	push @_, $md;
	goto &_do_dispatch;
}


sub _do_dispatch {
	my ($con, $request, $md) = @_;
	my $method = $request->{method};
	my $id = $request->{id};

	#return _handle_request($c, $request, $md);

	my $backend = $md->{b};
	my ($bns) = split /\./, $backend, 2;
	my $l; 

	if (my $fk = $md->{_f}) { #sel('backendfilter', $backend, $txn)
			# // sel('backendfilter', "$bns.*", $txn)) {

		$log->debug("filtering for $backend with $fk") if $debug;
		my $p = $request->{params};

		unless (is_hashref($p)) {
			return _error($con, $id, ERR_BADPARAM, "Parameters should be a json object for filtering.");
		}

		my $fv = $p->{$fk};

		unless (defined($fv)) {
			return _error($con, $id, ERR_BADPARAM, "filter parameter $fk undefined.")
		}

		#$l = $workermethods->{$backend}->{$fv};
		$l = allforkey('wm', "$backend\n$fv");
		return _error($con, $id, ERR_NOWORKER,
			"No worker available after filtering on $fk for backend $backend")
				unless $l and @$l;
	} else {
		#$l = $workermethods->{$backend};
		#$l = $env->db->query('select val from wm where method = ?', $backend)->arrays;
		$l = allforkey('wm', $backend);
		return _error($con, $id, ERR_NOWORKER, "No worker available for $backend.")
			unless $l and @$l;
		#$l = $l->map(sub{ print "map ", Dumper($_); $_ = decode_json($_->[0]); return $_ }); #->each;
	}

	#$log->debug('l: ', Dumper($l)) if $debug;

	my $wm;
	if ($#$l) { # only do expensive calculations when we have to
		#print 'workermethods: ', Dumper(@$l);
		# rotate workermethods
		#push @$l, shift @$l;
		# sort $l by refcount
		#$wm = (sort { $a->{connection}->{refcount} <=> $b->{connection}->{refcount}} @$l)[0];
		# this should produce least refcount round robin balancing
		$wm = (shuffle(@$l))[0];
	} else {
		$wm = $$l[0];
	}
	return _error($con, $id, ERR_INTERNAL, 'Internal error.') unless $wm;

	my $wcid = $wm->{cid};
	my $wcon;
	if ($child == $wm->{child}) {
		$wcon = $cons{$wcid};
		unless ($wcon) {
			$log->info('l: ', Dumper($l));
			die "wcon $wcid not found!?";
		}
	} else {
		$wcon = $mq[$wm->{child}] or
			die "mq for $wm->{child} not found!?";
	}
	die "no such wcon $wcid" unless $wcon->{cid};
	#$log->debug('wcon: ' . Dumper($wcon));
	#my $wcon = sel('cons', $wcid);
	$log->debug("forwarding $method to $wcon->{workername} ($wcid) for $backend")
		if $debug;

	# find or create channel
	#my $vci = md5_base64(refaddr($con).':'.refaddr($wcon)); # should be unique for this instance?
	my $vci = "$con->{cid}<->$wcid"; # should be unique for this instance?
	my $channel;
	unless ($channel = $con->{channels}->{$vci}) {
		$channel = RPC::Switch::Channel->new(
			ccid => $con->{cid},
			client => $con,
			vci => $vci,
			wcid => $wcid,
			worker => $wcon,
			#refcount => 0,
			reqs => {},
		);
		$con->{channels}->{$vci} =
			$wcon->{channels}->{$vci} =
				$channel;
	}
	if ($id) {
		#$wcon->{refcount}++;
		$channel->{reqs}->{$id} = 1;
	}
	#$md->{'#'}++; # per method call stats
	$mstat{$method}++;

	# rewrite request to add rcpswitch information
	#my $workerrequest = encode_json({
	#	jsonrpc => '2.0',
	#	rpcswitch => {
	#		vcookie => 'eatme', # channel information version
	#		vci => $vci,
	#		who => $who,
	#	},
	#	method => $backend,
	#	params => $request->{params},
	#	id  => $id,
	#});

	# rewrite request to add rcpswitch information
	my $rpcswitch = $request->{rpcswitch};
	#print 'rpcswitch info: ', Dumper($rpcswitch);
	unless ($rpcswitch and $rpcswitch->{vcookie} and $rpcswitch->{vcookie} eq 'eatme' and not defined $rpcswitch->{vci}) {
		# fixme: throw an error on invalid rpcswitch information?
		# difficult because we might get here asynch.
		$rpcswitch = { vcookie => 'eatme' };
	}
	$rpcswitch->{vci} = $vci;
	$rpcswitch->{who} = $con->{who};
	if ($md->{e}) { # show visible acls on request
		#my $visacl = sel('who2visacl', $con->{who});
		$rpcswitch->{acls} = [ keys %{ sel('who2visacl', $con->{who}) // {} } ];
	}

	my $workerrequest = encode_json({
		jsonrpc => '2.0',
		rpcswitch => $rpcswitch,
		method => $backend,
		params => $request->{params},
		id  => $id,
	});

	# forward request to worker
	$log->debug("refcount channel $channel " . scalar keys %{$channel->{reqs}}) if $debug;

	#$wcon->_write(encode_json($workerrequest));
	if ($wcon->{is_mq}) {
		# send to another worker via mq
		_send_to_mq($wcon, {
			fromcid => $con->{cid},
			tocid => $wcid,
			vci => $vci,
			id => undef,
			dir => ( $id ? 1 : 0 ),
			fromchild => $child,
			chunk => $workerrequest,
		});
	} else {
		$log->debug('    writing: ' . decode_utf8($workerrequest)) if $debug;
		$wcon->{ns}->write($workerrequest);
	}
	#_send_response({ cid => $wcid, bla => 'bla', chunk => $workerrequest });
	return; # exlplicit empty return

}

sub _handle_channel {
	my ($con, $jsonr, $r) = @_;
	$log->debug('    in handle_channel') if $debug;
	my $rpcswitch = $r->{rpcswitch};
	my $id = $r->{id};
	my $vci;

	# fixme: error on a response?
	unless ($rpcswitch->{vcookie}
			 and $rpcswitch->{vcookie} eq 'eatme'
			 and $vci = $rpcswitch->{vci}) {
		$log->info("invalid channel information from $con"); # better error message?
		return;
	}

	#print 'rpcswitch: ', Dumper($rpcswitch);
	#print 'channels; ', Dumper($channels);
	my $chan = $con->{channels}->{$vci};
	unless (is_hashref($chan)) {
		$log->info("no such channel $vci from $con)");
		#$log->info("chan : " . Dumper($chan));
		return;
	}
	#my $chan = $sql->db->query('select val from channels where vci = ?', $vci)->array;
	#my $chan = sel('channels', $vci);
	my $cid = $con->{cid};
	my ($dir, $tocid, $tocon);
	#$log->debug("con $con chan: ", Dumper($chan));
	if ($cid eq $chan->{wcid}) {
		# worker to client
		$tocid = $chan->{ccid};
		$tocon = $chan->{client};
		$dir = -1;
	#} elsif ($cid eq $chan->{ccid}) {
	#	# client to worker
	#	$tocon = $chan->{worker};
	#	$tocid = $chan->{wcid};
	#	$dir = 1;
	} else {
	#	$chan = undef;
	#}
	#unless ($chan) {
		$log->info("channel information from $con for a client"); # better error message?
		return _error($con, $id, ERR_BADCHAN, 'Channel information is not allowed from clients');
		return;
	}		

	#my $refmod;
	# fixme: look at dir?
	#if ($id) {
	#	if ($r->{method}) {
	#		$chan->{reqs}->{$id} = $dir;
	#		#$refmod = 1;
	#	} else {
	#		#$con->{refcount}--;
	#		#$refmod = -1;
	#		delete $chan->{reqs}->{$id};
	#		#del('reqs', $vci, $id);
	#		#$sql->db->query('delete from reqs where vci = ? and id = ?', $vci, $id);
	#	}
	#}
	#delete $chan->{reqs}->{$id} if $id;
	if ($id) {
		delete $chan->{reqs}->{$id};
		#if (delete $chan->{reqs}->{$id}) {
		#	say "_handle_channel: deleted $id";
		#} else {
		#	say "_handle_channel: failed to delete $id";
		#}
	}
	if ($debug) {
		#$log->debug("refcount connection $con $con->{refcount}");
		$log->debug("refcount $chan " . scalar keys %{$chan->{reqs}});
		$log->debug('    writing: ' . decode_utf8($$jsonr));
		$log->debug("tocid $tocid, tocon $tocon, dir $dir");
	}
	# forward request
	# we could spare a encode here if we pass the original request along?
	#$con->_write(encode_json($r));
	if ($tocon->{is_mq}) {
		# send to another processor via mq
		_send_to_mq($tocon, {
			tocid => $tocid,
			vci => $vci,
			id => $id,
			#refmod => $refmod,
			dir => $dir,
			fromcid => $cid,
			fromchild => $child,
			chunk => $$jsonr,
		});
	} else {
		$tocon->{ns}->write($$jsonr);
	}
	#_send_response({ cid => $cid, chunk => $$jsonr });
	return;
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

sub _notify {
	my ($c, $name, $args) = @_;
	croak 'args should be a array of hash reference'
		unless is_arrayref($args) or is_hashref($args);
	$c->_write(encode_json({
		jsonrpc => '2.0',
		method => $name,
		params => $args,
	}));
	#$RPC::Switch::log->debug("    notify [$$]: $chunk");
	return;
}

sub _error {
	my ($c, $id, $code, $message, $data) = @_;
	return $c->_error($id, $code, $message, $data);
}

sub _disconnect {
	my ($con) = @_;
	#my $con = sel('cons', $cid);
	my $cid = $con->cid;

	$log->info('oh my.... ' . ($cid // 'unknown') 
				. ' (' . ($con->{who} // 'somebody')
				. ' ) from ' . ($con->{from} // 'somewhere') . ') disonnected..');

	delete $cons{$cid};
	del('cons', $cid);
	return unless $con->{who};
	#$log->debug("_disconnect: cid $cid con:", Dumper($con));

	my $wms = $con->{workermethods} // [];
	
	$log->debug("_disconnect: wms: " . Dumper($wms));

	for my $wm (@$wms) {
		# we need the json value here because we need the exact string value to delete
		my $foo = allforkey('wm', $wm, 'json');
		next unless $foo;
		#print "wm $wm: ", Dumper($foo);
		my @foo = grep { my $bar = decode_json($_); $bar->{cid} eq $cid } @$foo;
		del('wm', $wm, $foo[0]) if @foo;
	}

	for my $c (values %{$con->channels}) {
		#say 'contemplating ', $c;
		my $vci = $c->vci;
		unless ($vci) {
			$log->debug("uh? $c?");
			next;
		}
		my $reqs = $c->reqs;
		my ($tocid, $tocon, $dir);
		if ($cid eq $c->{wcid}) {
			# worker role in this channel: notify client
			$tocid = $c->{ccid};
			$tocon = $c->{client};
			$dir = -1;
		} elsif ($cid eq $c->{ccid}) {
			# client role in this channel: notify worker
			$tocon = $c->{worker};
			$tocid = $c->{wcid};
			$dir = 1;
		}
		#if (refaddr $c->worker == refaddr $client) {
		#	# worker role in this channel: notify client 
		#	$con = $c->client;
		#	$dir = 1;
		#} else {
		#	# notify worker
		#	$con = $c->worker;
		#	$dir = -1;
		#}
		$log->debug("tocon $tocon tocid $tocid dir $dir");
		if ($tocon->is_mq) {
			# send to another processor via mq
			_send_to_mq($tocon, {
				channel_gone => $vci,
				tocid => $tocid,
				fromcid => $cid,
				fromchild => $child,
				dir => $dir,
			});
		} else {
			if ($dir == -1) {
				for my $id (keys %$reqs) {
					#say "looking at $id: ", $reqs->{$id};
					$tocon->_error($id, ERR_GONE, 'opposite end of channel gone');
				}
			}
			$tocon->notify('rpcswitch.channel_gone', {channel => $vci});
		}
		delete $tocon->channels->{$vci};
		#print 'tocon: ', Dumper($tocon);
		$log->debug("_disconnect: delete channel $c");
		$c->delete();
	}

	$log->debug("_disconnect done for $con?");
	return;
}

sub _channel_gone {
	my ($msg) = @_;
	my ($vci, $tocid, $fromcid, $fromchild, $dir) = @$msg{qw(channel_gone tocid fromcid fromchild dir)};

	my $tocon = $cons{$tocid};
	return unless $tocon;

	my $c = delete $tocon->{channels}->{$vci};

	$log->info("forwarding channel_gone to $tocid ($tocon) for channel $vci ($c) dir $dir");
	my $reqs = $c->reqs;

	if ($dir == -1) {
		for my $id (keys %$reqs) {
			#say "looking at $id: ", $reqs->{$id};
			$tocon->_error($id, ERR_GONE, 'opposite end of channel gone');
		}
	}
	$tocon->notify('rpcswitch.channel_gone', {channel => $vci});
	$log->debug("_channel_gone: delete channel $c");
	$c->delete();

	return;
}

sub _write_stats {
	my $fn = "$tmpdir/st/$child.json";
	my $nfn = "$fn.new";

	my %stats = (
		chunks => $chunks,
		connections => $concount,
		clients => scalar keys %cons,
		methods => \%mstat,
		workers => scalar grep { is_hashref($_) and $_->{workermethods} } values %cons,
	);

	open(my $fh, '>', "$nfn")
                    or die "Can't open > $nfn: $!";
	say $fh encode_json(\%stats);
	close($fh);
	rename($nfn, $fn)
		or die "Can't rename $nfn to $fn: $!";

}

1;

__END__


sub _handle {
	my ($c, $jsonr) = @_;
	weaken $jsonr;
	$log->debug("    handle [$c]: $$jsonr") if $debug;
	local $@;
	my $r = eval { decode_json($$jsonr) };
	return error($c, undef, ERR_PARSE, "json decode failed: $@")
		 if $@;
	return error($c, undef, ERR_REQ, 'Invalid Request: not a json object')
		 unless is_hashref($r);
	return error($c, undef, ERR_REQ, 'Invalid Request: expected jsonrpc version 2.0')
		 unless defined $r->{jsonrpc} and $r->{jsonrpc} eq '2.0';
	return error($c, undef, ERR_REQ, 'Invalid Request: id is not a string or number')
		if exists $r->{id} and ref $r->{id};		
	$RPC::Switch::chunks++;
	if (defined $r->{rpcswitch}->{vci}) {
		return _handle_channel($c, $jsonr, $r);
	} elsif (defined $r->{method}) {
		return _handle_request($c, $r);
	} elsif (exists $r->{id} and (exists $r->{result} or defined $r->{error})) {
		# bounce back to io process
		_send_response({ cid => $c, resp => $r });
		return;
	} else {
		return error($c, undef, ERR_REQ, 'Invalid Request: invalid jsonnrpc object');
	}
}
