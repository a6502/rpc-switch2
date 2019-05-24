package RPC::Switch::Auth;
use Mojo::Base -strict;

use Module::Load;
use Scalar::Util qw(blessed);

our %methods;

sub init {
	my ($cfgdir, $cfg, $section) = @_;

	my $ms = $cfg->{$section};
	keys %$ms;
	
	while (my ($m, $mod) = each %$ms) {
		load $mod;
		my $a = $mod->new($cfgdir, $cfg->{"$section|$m"});
		die "could not create authentication adapter '$mod'"
			unless blessed($a) and $a->can('authenticate');
		$methods{$m} = $a;
	}

	return 1;
}


sub authenticate {
	my ($method, $con, $who, $token, $cb) = @_;

	$cb->(0, 'undef argument(s)') unless $who and $method and $token;
	
	my $adapter = $methods{$method};
	unless ($adapter) {
		$cb->(0, "no such authentication method '$method' for server '$con->{server}->{localname}'");
		return;
	}

	my @res = $adapter->authenticate($con, $who, $token, $cb);
	
	$cb->(@res) if @res;
}

1;
