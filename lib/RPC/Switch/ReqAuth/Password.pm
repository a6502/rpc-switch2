package RPC::Switch::ReqAuth::Password;
use Mojo::Base -base;

use Data::Dumper;

use Digest::SHA ();
use MIME::Base64 ();

has [qw(passwd reqs)];

sub new {
	my $self = shift->SUPER::new();

	my ($cfgdir, $cfg) = @_;

	my $pwfile = $cfg->{pwfile} or die "no pwfile";
	
	die "pwfile $pwfile does not exist" unless -r "$cfgdir/$pwfile";
	
	my %passwd;
	open my $fh, '<', "$cfgdir/$pwfile" or return (0, 'cannot open pwfile');
	while (<$fh>) {
		chomp;
		next if /^#/;
		my ($u, $e) = split /:/;
		$passwd{$u} = $e;
	}
	close $fh;

	$self->{passwd} = \%passwd;
	$self->{reqs} = 0;
	return $self;
}


sub authenticate_request {
	my ($self, $reqauth, $cb) = @_;
	#print 'passwd auth ', Dumper(@_);
	my ($user, $pass) = @$reqauth{qw(user password)};

	return (0, 'undef argument(s)') unless $user and $pass;
		
	return (0, 'auth queue overflow ' . $self->{reqs} )
		if ( $self->{reqs} > 5000 );
	++$self->{reqs};

	my $encrypted = $self->{passwd}->{$user};

	return (0, 'no such user') unless $encrypted;
	
	return (0, 'invalid password hash') unless $encrypted =~ /^(\$[156]\$[^\$]+\$)/;
	my $salt = $1;

	#return {
	#	auth_type => 'password', # todo: var
	#	auth_info => 'whoohoo',
	#	validated_scope => {
	#		user => $user
	#	},
	#} if crypt( $pass, $salt ) eq $encrypted;
	#return (0, 'password mismatch');

	$RPC::Switch::ioloop->timer(.1 => sub {
		--$self->{reqs};
		return $cb->({
			auth_type => 'password', # todo: var?
			auth_info => 'whoohoo',
			validated_scope => {
				user => $user
			},
		}) if crypt( $pass, $salt ) eq $encrypted;
		$cb->(0, 'password mismatch');
	});
	return;
}

1;

=pod

Create the encrypted password with "mkpasswd --method=sha-256 <pw>"

=cut
