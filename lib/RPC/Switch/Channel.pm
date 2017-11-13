package RPC::Switch::Channel;
use Mojo::Base 'Mojo::EventEmitter';

has [qw(client reqs vci worker)];

sub delete {
	my $self = shift;
	%$self = ();
}

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
