package RPC::Switch::Connection;
use Mojo::Base 'Mojo::EventEmitter';
use Mojo::IOLoop;

# standard perl
use Carp;
use Data::Dumper;
use Digest::MD5 qw(md5_base64);
use Encode qw(encode_utf8 decode_utf8);
use Scalar::Util qw(refaddr);

# cpan
use JSON::MaybeXS;
use MojoX::NetstringStream 0.05;

# keep in sync with RPC::Switch
use constant {
	ERR_TOOBIG   => -32010, # Req/Resp object too big
	ERR_REQ      => -32600, # The JSON sent is not a valid Request object 
	ERR_PARSE    => -32700, # Invalid JSON was received by the server.
};

has [qw(channels debug from localname log methods ns ping refcount
	reqauth state stream switch tmr who workername worker_id)];

sub new {
	my $self = shift->SUPER::new();
	my ($switch, $stream, $localname) = @_;
	#say 'new connection!';
	die 'no stream?' unless $stream and $stream->can('write');
	my $handle = $stream->handle;
	my $from = $handle->peerhost .':'. $handle->peerport;
	my $ns = MojoX::NetstringStream->new(
		stream => $stream,
		maxsize => 999999, # fixme: configurable?
	);
	$ns->on(chunk => sub {
		my ($ns, $chunk) = @_;
		# Process input chunk
		$self->log->debug("    handle: " . decode_utf8($chunk)) if $self->{debug};
		local $@;
		my $r = eval { decode_json($chunk) };
		my @err;
		if ($@) {
			@err = $self->_error(undef, ERR_PARSE, "json decode failed: $@");
		} else {
			@err = $self->_handle(\$chunk, $r);
			@err = $self->_error(undef, ERR_REQ, 'Invalid Request: ' . $err[0])
				if $err[0];
		}
		$self->log->error(join(' ', grep defined, @err[1..$#err])) if @err;
		$self->close if $err[0];
		return;
	});
	$ns->on(close => sub { $self->_on_close(@_) });
	$ns->on(nserr => sub {
		my ($ns, $msg) = @_;
		$self->log->error("$from ($self): $msg");
		$self->_error(undef, ERR_TOOBIG, $msg);
		$self->close;
	});

	$self->{channels} = {};
	$self->{debug} = $switch->{debug};
	$self->{from} = $from;
	$self->{localname} = $localname;
	$self->{log} = $switch->log;
	$self->{ns} = $ns;
	$self->{ping} = 60; # fixme: configurable?
	$self->{methods} = {};
	$self->{refcount} = 0;
	$self->{stream} = $stream;
	$self->{switch} = $switch;

	$self->log->info('new connection on '. $localname . ' (' . $self .') from '. $from);

	# fixme: put this in a less hidden place?
	$self->notify('rpcswitch.greetings', {who =>'rpcswitch', version => '1.0'});

	return $self;
}

sub call {
	my ($self, $name, $args, $cb, %opts) = @_;
	croak 'no self?' unless $self;
	croak 'args should be a array or hash reference'
		unless ref $args eq 'ARRAY' or ref $args eq 'HASH';
	croak 'no callback?' unless $cb;
	croak 'callback should be a code reference' if ref $cb ne 'CODE';
	my $id = md5_base64($self->{next_id}++ . $name . encode_json($args) . refaddr($cb));
	croak 'duplicate call id' if $self->{calls}->{$id};
	
	my $request = {
		jsonrpc => '2.0',
		method => $name,
		params => $args,
		id  => $id,
	};
	$request->{vci} = $opts{vci} if $opts{vci};
	$request = encode_json($request);
	$self->{calls}->{$id} = $cb; # more?
	#$self->log->debug("    call: $request");
	$self->_write($request);
	return;
}

sub notify {
	my ($self, $name, $args, $cb) = @_;
	croak 'no self?' unless $self;
	croak 'args should be a array of hash reference'
		unless ref $args eq 'ARRAY' or ref $args eq 'HASH';
	my $request = encode_json({
		jsonrpc => '2.0',
		method => $name,
		params => $args,
	});
	#$self->log->debug("    notify: $request");
	$self->_write($request);
	return;
}

#sub handle {
#	my ($self, $json) = @_;
#	$self->log->debug("    handle: $json") if $self->{debug};
#	local $@;
#	my $r = eval { decode_json($json) };
#	return $self->_error(undef, ERR_PARSE, "json decode failed: $@") if $@;
#	my @err = $self->_handle($r);
#	return $self->_error(undef, ERR_REQ, 'Invalid Request: ' . $err[0]) if $err[0];
#        return @err;
#}

sub _handle {
	my ($self, $jsonr, $r) = @_;
	return 'not a json object' if ref $r ne 'HASH';
	return 'expected jsonrpc version 2.0' unless defined $r->{jsonrpc} and $r->{jsonrpc} eq '2.0';
	# id can be null
	#return 'id is not a string or number' if exists $r->{id} and (not defined $r->{id} or ref $r->{id});
	return 'id is not a string or number' if exists $r->{id} and ref $r->{id};
	if (defined $r->{rpcswitch}) {
		return $self->{switch}->_handle_channel($self, $jsonr, $r);
	} elsif (defined $r->{method}) {
		return $self->{switch}->_handle_request($self, $r);
	} elsif (exists $r->{id} and (exists $r->{result} or defined $r->{error})) {
		return $self->_handle_response($r);
	} else {
		return 'invalid jsonnrpc object';
	}
}

sub _handle_response {
	my ($self, $r) = @_;
	#$self->log->debug('_handle_response: '. Dumper ($r));
	my $id = $r->{id};
	my $cb;
	$cb = delete $self->{calls}->{$id} if $id;
	if (defined $r->{error}) {
		my $e = $r->{error};
		return 'error is not an object' unless ref $e eq 'HASH';
		return 'error code is not a integer' unless defined $e->{code} and $e->{code} =~ /^-?\d+$/;
        	return 'error message is not a string' if ref $e->{message};
        	return 'extra members in error object' if (keys %$e == 3 and !exists $e->{data}) or (keys %$e > 2);
		$cb->($r->{error}) if $cb and ref $cb eq 'CODE';
	} else {
		return undef, "response for unknown call $id" unless $cb and ref $cb eq 'CODE';
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
	return 1, $err if $@;
	return 1, $err if $code == ERR_PARSE or $code == ERR_REQ;
	return 0, $err;
}

sub _write {
	my $self = shift;
	$self->log->debug('    writing: ' . decode_utf8(join('', @_)))
		if $self->{debug};
	$self->{ns}->write(@_);
}

sub _on_close {
	my ($self, $ns) = @_;
	Mojo::IOLoop->remove($self->{tmr}) if $self->{tmr};
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
