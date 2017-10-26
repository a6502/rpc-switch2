package RPC::Switch::Connection;
use Mojo::Base 'Mojo::EventEmitter';
use Mojo::IOLoop;

# standard perl
use Carp;
use Data::Dumper;
use Digest::MD5 qw(md5_base64);
use Scalar::Util qw(refaddr);

# cpan
use JSON::MaybeXS;
use MojoX::NetstringStream;

use constant {
	ERR_REQ      => -32600, # The JSON sent is not a valid Request object 
	ERR_PARSE    => -32700, # Invalid JSON was received by the server.
};

has [qw(channels id log methods ns owner ping reqauth state stream switch tmr who workername worker_id)];

sub new {
	my $self = shift->SUPER::new();
	my ($switch, $stream, $id) = @_;
	#say 'new connection!';
	die 'no stream?' unless $stream and $stream->can('write');
	my $ns = MojoX::NetstringStream->new(
		stream => $stream
	);
	#my $con = $rpc->newconnection(
	#	owner => $self,
	#	write => sub { $ns->write(@_) },
	#);
	$ns->on(chunk => sub {
		my ($ns, $chunk) = @_;
		# Process input chunk
		#$self->log->debug("    got chunk: $chunk");
		#my @err = $con->handle($chunk);
		my @err = $self->handle($chunk);
		$self->log->error(join(' ', grep defined, @err[1..$#err])) if @err;
		#$ns->close if $err[0];
		$self->close if $err[0];
	});
	$ns->on(close => sub { $self->_on_close(@_) });
	
	$self->{channels} = {};
	$self->{id} = $id;
	$self->{log} = $switch->log;
	$self->{ns} = $ns;
	$self->{ping} = 60; # fixme: configurable?
	$self->{methods} = {};
	$self->{stream} = $stream;
	$self->{switch} = $switch;

	my $handle = $stream->handle;
	$self->log->info('new connection '. $self .' from '. $handle->peerhost .':'. $handle->peerport);

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
		rpcswitch => JSON->true,
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

sub handle {
	my ($self, $json) = @_;
	$self->log->debug("    handle: $json");
	local $@;
	my $r = eval { decode_json($json) };
	return $self->_error(undef, ERR_PARSE, "json decode failed: $@") if $@;
	my @err = $self->_handle($r);
	return $self->_error(undef, ERR_REQ, 'Invalid Request: ' . $err[0]) if $err[0];
        return @err;
}

sub _handle {
	my ($self, $r) = @_;
	return 'not a json object' if ref $r ne 'HASH';
	return 'expected jsonrpc version 2.0' unless defined $r->{jsonrpc} and $r->{jsonrpc} eq '2.0';
	return 'id is not a string or number' if exists $r->{id} and (not defined $r->{id} or ref $r->{id});
	if (defined $r->{rpcswitch}) {
		return $self->switch->_handle_channel($self, $r);
	} elsif (defined $r->{method}) {
		return $self->switch->_handle_request($self, $r);
	} elsif (defined $r->{id} and (exists $r->{result} or defined $r->{error})) {
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
	$self->log->debug('    writing: ' . join('', @_));
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

sub DESTROY {
	my $self = shift;
	say 'destroying ', $self;
	%$self = ();
}

1;
