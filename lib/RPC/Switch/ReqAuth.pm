package RPC::Switch::ReqAuth;

use Mojo::Base -strict;

use Data::Dumper;
use Module::Load;
use List::Util qw(any);
use Scalar::Util qw(blessed);

use Ref::Util qw(is_arrayref is_coderef is_hashref);

#has [qw(types)];
our %types;

sub init {
#sub new {
	#my $self = shift->SUPER::new();
	my (%args) = @_;
	my ($cfgdir, $cfg, $section) = @args{qw(cfgdir cfg cfgsection)};

	my $ts = $cfg->{$section};
	keys %$ts;
	
	while (my ($t, $mod) = each %$ts) {
		#say "trying to load $mod";
		load $mod;
		my $h = $mod->new($cfgdir, $cfg->{"$section|$t"});
		die "could not create request authentication handler '$mod'"
			unless blessed($h) and $h->can('authenticate_request');
		$types{$t} = $h;
	}

	#$self->{types} = $types;
	#return $self;
	return 1;
}

sub authenticate_request {
	my ($ras, $ra, $cb) = @_;
	#print 'in authenticate_request: ' . Dumper($ra);
	my ($at, $h);
	unless (is_hashref($ra) and $at = $ra->{auth_type}) {
		return (0, 'invalid reqauth object');	
	}
	unless (any { $_ eq $at } @$ras) {
		return (0, "reqauth type '$at' not in list of allowed types '" . join(', ', @$ras) . "'");
	}
	unless ($h = $types{$at}) {
		return (0, "no handler for reqauth type '$at'");
	}
	return $h->authenticate_request($ra, $cb);
}

1;

