package RPC::Switch::Channel;
use Mojo::Base -base;

has [qw(ccid client reqs vci wcid worker)];

sub delete {
	my $self = shift;
	%$self = ();
}

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
