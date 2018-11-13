package RPC::Switch::Auth;
use Mojo::Base 'Mojo::EventEmitter';

use Module::Load;

has [qw(methods)];

sub new {
	my $self = shift->SUPER::new();

	my ($cfgdir, $cfg, $section) = @_;

	my $methods = $cfg->{$section};
	
	for my $m (keys %$methods) {
		my $mod = $methods->{$m};
		load $mod;
		my $a = $mod->new($cfgdir, $cfg->{"$section|$m"});
		die "could not create authentication module object '$mod'" unless $a;
		$methods->{$m} = $a;
	}

	$self->{methods} = $methods;
	return $self;
}


sub authenticate {
	my ($self, $method, $con, $who, $token, $cb) = @_;

	$cb->(0, 'undef argument(s)') unless $who and $method and $token;
	
	my $adapter = $con->server->authmethods->{$method};
	unless ($adapter) {
		$cb->(0, "no such authentication method '$method' for server '$con->{server}->{localname}'");
		return;
	}

	my @res = $adapter->authenticate($con, $who, $token, $cb);
	
	$cb->(@res) if @res;
}

1;
