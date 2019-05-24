package RPC::Switch::MQ;
use Mojo::Base 'MojoX::POSIX_RT_MQ';

#
# Make a MojoX::POSIX_RT_MQ look a bit like a RPC::Switch::Connection.
#

has [qw(channels cid is_mq refcount workername)];

sub delete {
	my $self = shift;
	%$self = ();
}

#sub DESTROY {
#	my $self = shift;
#	say 'destroying ', $self;
#}

1;
