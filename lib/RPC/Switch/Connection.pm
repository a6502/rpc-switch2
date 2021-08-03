package RPC::Switch::Connection;
use Mojo::Base 'Mojo::EventEmitter';
#use Mojo::IOLoop;

# standard perl
use Carp;
use Data::Dumper;
use Digest::MD5 qw(md5_base64);
use Encode qw(encode_utf8 decode_utf8);
use Scalar::Util qw(refaddr);

# cpan
use JSON::MaybeXS;
use MojoX::NetstringStream 0.05;
use Ref::Util qw(is_arrayref is_coderef is_hashref);

# keep in sync with RPC::Switch
use constant {
	ERR_TOOBIG   => -32010, # Req/Resp object too big
	ERR_REQ      => -32600, # The JSON sent is not a valid Request object 
	ERR_PARSE    => -32700, # Invalid JSON was received by the server.
};

has [qw(channels cid from is_mq methods ncid ns ping refcount reqauth reqq server
	state stream tmr who workername worker_id)];

#our $connection_id_counter = 1;
# (shared) globals
our (
	$chunks,
	$concount,
	$debug,
	$ioloop,
	$log,
);

sub new {
	my $self = shift->SUPER::new();
	my ($server, $stream) = @_;
	#say 'new connection!';
	die 'no stream?' unless $stream and $stream->can('write');
	my $handle = $stream->handle;
	my $from = $handle->peerhost .':'. $handle->peerport;
	$concount++;
	my $ncid = $self->{ncid} = $concount; # $connection_id_counter; # numerical connection id
	my $cid = $self->{cid} = "$ncid\@$$";
	#$connection_id_counter++;
	my $ns = MojoX::NetstringStream->new(
		stream => $stream,
		maxsize => 999999, # fixme: configurable?
	);
	$ns->on(chunk => sub {
		my ($ns, $chunk) = @_;
		# Process input chunk
		$log->debug("    handle: " . decode_utf8($chunk)) if $debug;
		# todo: try catch?
		local $@;
		my $r = eval { decode_json($chunk) };
		my @err;
		if ($@) {
			@err = $self->_error(undef, ERR_PARSE, "JSON decode failed: $@");
			$log->error(join(' ', grep defined, @err[1..$#err]));
			$self->close;
			return;
		}
		@err = $self->_handle(\$chunk, $r);
		return unless @err;
		@err = $self->_error(undef, ERR_REQ, ('Invalid Request: ' . join(' ', grep defined, @err)) )
			unless ref $err[0];
		$log->error(join(' ', grep defined, @err[1..$#err])) if @err;
		if (ref $err[0] and ${$err[0]}) {
			$log->error("Closing connection $cid because of errors");
			$self->close; # if $err[0];
		}
		return;
	});
	$ns->on(close => sub { $self->_on_close(@_) });
	$ns->on(nserr => sub {
		my ($ns, $msg) = @_;
		$log->error("$from ($self): $msg");
		$self->_error(undef, ERR_TOOBIG, $msg);
		$self->close;
	});

	$self->{channels} = {};
	$self->{from} = $from;
	$self->{is_mq} = 0;
	$self->{ns} = $ns;
	$self->{ping} = 60; # fixme: configurable?
	$self->{methods} = {};
	$self->{refcount} = 0;
	$self->{server} = $server;
	$self->{stream} = $stream;

	$log->info("new connection on $server->{localname} ($cid / $self) from  $from");

	# fixme: put this in a less hidden place?
	$self->notify('rpcswitch.greetings', {who =>'rpcswitch', version => '1.0'});

	return $self;
}

sub _handle {
	my ($self, $jsonr, $r) = @_;
	return 'not a json object' unless is_hashref($r);
	return 'expected jsonrpc version 2.0' unless defined $r->{jsonrpc} and $r->{jsonrpc} eq '2.0';
	# id can be null
	#return 'id is not a string or number' if exists $r->{id} and (not defined $r->{id} or ref $r->{id});
	return 'id is not a string or number' if exists $r->{id} and ref $r->{id};
	$chunks++;
	if (is_hashref($r->{rpcswitch}) and $r->{rpcswitch}{vci}) {
		#return RPC::Switch::Processor::_handle_channel($self, $jsonr, $r);
		goto &RPC::Switch::Processor::_handle_channel;
	} elsif (defined $r->{method}) {
		#return RPC::Switch::Processor::_handle_request($self, $r);
		@_ = ($self, $r);
		goto &RPC::Switch::Processor::_handle_request;
	} elsif (exists $r->{id} and (exists $r->{result} or defined $r->{error})) {
		#return $self->_handle_response($r);
		@_ = ($self, $r);
		goto &_handle_response;
	} else {
		return 'invalid jsonnrpc object';
	}
}

sub call {
	my ($self, $name, $args, $cb, %opts) = @_;
	croak 'no self?' unless $self;
	croak 'args should be a array or hash reference'
		unless ref $args eq 'ARRAY' or ref $args eq 'HASH';
	croak 'no callback?' unless $cb;
	croak 'callback should be a code reference' unless is_coderef($cb);
	my $id = md5_base64($self->{next_id}++ . $name . encode_json($args) . refaddr($cb));
	croak 'duplicate call id' if $self->{calls}->{$id};
	
	my $request = {
		jsonrpc => '2.0',
		method => $name,
		params => $args,
		id  => $id,
	};
	#$request->{vci} = $opts{vci} if $opts{vci};
	$request = encode_json($request);
	$self->{calls}->{$id} = $cb; # more?
	#$log->debug("    call: $request");
	$self->_write($request);
	return;
}

sub notify {
	my ($self, $name, $args, $cb) = @_;
	croak 'no self?' unless $self;
	croak 'args should be a array of hash reference'
		unless is_arrayref($args) or is_hashref($args);
	my $request = encode_json({
		jsonrpc => '2.0',
		method => $name,
		params => $args,
	});
	#$log->debug("    notify: $request");
	$self->_write($request);
	return;
}

sub _handle_response {
	my ($self, $r) = @_;
	#$log->debug('_handle_response: '. Dumper ($r));
	my $id = $r->{id};
	my $cb;
	$cb = delete $self->{calls}->{$id} if $id;
	if (defined $r->{error}) {
		my $e = $r->{error};
		return 'error is not an object' unless is_hashref($e);
		return 'error code is not a integer' unless defined $e->{code} and $e->{code} =~ /^-?\d+$/;
        	return 'error message is not a string' if ref $e->{message};
        	return 'extra members in error object' if (keys %$e == 3 and !exists $e->{data}) or (keys %$e > 3);
		$cb->($r->{error}) if $cb and is_coderef($cb);
	} else {
		return undef, "response for unknown call $id" unless $cb and is_coderef($cb);
		$cb->(0, $r->{result});
	}
	return;
}

sub _error {
	my ($self, $id, $code, $message, $data) = @_;
	my $err = "error: $code " . $message // '';
	#$self->log->error($err);
	local $@;
	eval {
		$self->_write(encode_json({
			jsonrpc     => '2.0',
			id          => $id,
			error       => {
				code        => $code,
				message     => $message,
				(defined $data ? ( data => $data ) : ()),
			},
		}));
	};
	return \1, $err if $@;
	return \1, $err if $code == ERR_PARSE or $code == ERR_REQ;
	return \0, $err;
}

sub _write {
	my $self = shift;
	$log->debug("    writing [$self->{cid}]: " . decode_utf8(join('', @_)))
		if $debug;
	$self->{ns}->write(@_);
}

sub _on_close {
	my ($self, $ns) = @_;
	$ioloop->remove($self->{tmr}) if $self->{tmr};
	$self->emit(close => $self);
	%$self = ();
}

sub close {
	my ($self) = @_;
	$self->stream->close_gracefully;
}

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;

__END__
