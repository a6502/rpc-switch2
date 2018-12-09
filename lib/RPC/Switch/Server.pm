package RPC::Switch::Server;
use Mojo::Base -base;

use Scalar::Util qw(refaddr);

has [qw(authmethods localname server)];

sub new {
	my $self = shift->SUPER::new();
	my ($l) = @_;

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

	my $am = $RPC::Switch::auth->methods;
	if ($l->{auth}) {
		my %authmethods;
		for (@{$l->{auth}}) {
			die "Unkown auth method $_" unless $authmethods{$_} = $am->{$_};
		}
		$self->{authmethods} = \%authmethods;
	} else {
		$self->{authmethods} = $am;
	}

	my $localname = $l->{name} // (($serveropts->{address} // '0') . ':' . $serveropts->{port});

	my $server = $RPC::Switch::ioloop->server(
		$serveropts => sub {
			my ($loop, $stream, $id) = @_;
			my $client = RPC::Switch::Connection->new($self, $stream);
			$client->on(close => sub { RPC::Switch::_disconnect($client) });
			$RPC::Switch::connections++;
			$RPC::Switch::clients->{refaddr($client)} = $client;
		}
	) or die 'no server?';

	$self->{localname} = $localname;
	$self->{server} = $server;

	return $self;
}


#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
