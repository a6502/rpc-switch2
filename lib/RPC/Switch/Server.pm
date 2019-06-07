package RPC::Switch::Server;
use Mojo::Base -base;

use Data::Dumper;
use Scalar::Util qw(refaddr);

has [qw(authmethods id localname server)];

our (
	%cons,
	$ioloop,
);

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
		$serveropts->{tls_verify} = 0x03;
		$serveropts->{tls_ca} = $l->{tls_ca};
	}

	my $am = \%RPC::Switch::Auth::methods;
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
	print "serveropts: ", Dumper($serveropts);

	my $master = $$;

	my $id = $RPC::Switch::ioloop->server(
		$serveropts => sub {
			my ($loop, $stream, $id) = @_;
			die "accepted in master!?" if $$ == $master;
			my $client = RPC::Switch::Connection->new($self, $stream);
			$client->on(close => sub { RPC::Switch::Processor::_disconnect($client) });
			$cons{$client->cid} = $client;
			# make some interesting information 'global'
			RPC::Switch::ins('cons', $client->cid, {
				from => $client->from,
				localname => $client->server->localname,
			});
		}
	) or die 'no server?';

	say "got server $id";
	$self->{localname} = $localname;
	$self->{server} = $ioloop->acceptor($id);
	$self->{id} = $id;

	return $self;
}

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
