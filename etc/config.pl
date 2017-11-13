
#bla?

$cfg = {
	methods => 'methods.pl',
	listen => [
		{
			name => 'foo',
			# default
			#address => '127.0.0.1',
			#port => 6551,
		},
		{
			address => '127.0.0.1',
			port => 6850,
			#tls_ca => '/home/wieger/src/jobcenter/etc/JobcenterCA.crt',
			tls_cert => '/home/wieger/src/jobcenter/etc/wsworker.crt',
			tls_key => '/home/wieger/src/jobcenter/etc/wsworker.pem',
		},
	],
	auth => {
		password => 'RPC::Switch::Auth::Passwd',
	},
	'auth|password' => {
		pwfile => 'api.passwd'
	}
};


