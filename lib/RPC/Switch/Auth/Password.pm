package RPC::Switch::Auth::Password;
use Mojo::Base 'RPC::Switch::Auth::Base';

use Data::Dumper;

use Digest::SHA ();
use MIME::Base64 ();

has [qw(pwfile)];

sub new {
	my $self = shift->SUPER::new();

	my ($cfgdir, $cfg) = @_;

	my $pwfile = $cfg->{pwfile} or die "no pwfile";
	
	die "pwfile $pwfile does not exist" unless -r "$cfgdir/$pwfile";

	$self->{pwfile} = "$cfgdir/$pwfile";
	return $self;
}


sub authenticate {
	my ($self, $connection, $who, $token) = @_;

	#print 'auth ', Dumper(@_);

	return (0, 'undef argument(s)') unless $connection and $who and $token;
		
	my $encrypted;
	open my $fh, '<', $self->pwfile or return (0, 'cannot open pwfile');
	while (<$fh>) {
		chomp;
		my ($u, $e) = split /:/;
		if ($u eq $who) {
			$encrypted = $e;
			last;
		}
	}
	close $fh;

	return (0, 'no such user') unless $encrypted;
	
	return (0, 'invalid password hash') unless $encrypted =~ /^(\$[156]\$[^\$]+\$)/;

	my $salt = $1;

        return (1, 'whoohoo') if crypt( $token, $salt ) eq $encrypted;

	return (0, 'nope');
}

1;

=pod

Create the encrypted password with "mkpasswd --method=sha-256 <pw>"

=cut
