package RPC::Switch::WorkerMethod;
use Mojo::Base -base;

has [qw(connection doc filterkey filtervalue method)];

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
