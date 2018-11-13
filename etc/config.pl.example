
# example config

$cfg = {
	# methods configuration file to load
	methods => 'methods.pl',
	listen => [
		{
			# dislay name for this local endpoint
			name => 'default port',
			# default values:
			#address => '127.0.0.1',
			#port => 6551,
			# auth => ['all'],
		},
		{
			address => '127.0.0.1',
			port => 6850,
			# adding a key enables tls
			tls_cert => 'rpcswitch.crt',
			tls_key => 'rpcswitch.pem',
			# adding a ca enables client cerfiticate checking
			tls_ca => 'myCA.crt',
		},
	],
	auth => {
		# authentication methods supported
		password => 'RPC::Switch::Auth::Passwd',
		clientcert => 'RPC::Switch::Auth::ClientCert',
	},
	# per authentication method configuration
	'auth|password' => {
		pwfile => 'switch.passwd'
	},
	'auth|clientcert' => {
		cnfile => 'switch.cnfile'
	},
};

