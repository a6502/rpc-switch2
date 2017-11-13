
#bla!

# single level namespace mapping
$methods = {
	#namespace
	'prov' => {
		# method => backend
		'update_domain' => 'midat_worker.'
	},
	'foo' => {
		'power' => 'bar.square',
		'add' => 'bar.',
		'div' => 'bar.',
	},
};

$acl = {
	# acl => [+otheracl, user]
	'provold' => [qw( bergholt stuttley )],
	'provnew' => [qw( devere nodder sladegore )],
	'provjc' => [qw( johnson )],
	'prov' => ['+provnew', '+provold'],
	'addbar' => ['+bar', 'deArbeider'],
	'bar' => [qw( theEmployee derArbeitnehmer )],
	'klant' => [qw( deKlant theCustomer )],
	'public' => '*', # implicit special case?
};

# which acls are allowed to call which methods
$method2acl = {
	# namespace.method => acl
	'foo.div' => ['klant','prov'],
	'foo.*'  => 'public',
	'prov.*' => ['provold', 'provnew', 'provjc'],
};

# which acls are allowed to announce which methods
$backend2acl = {
	# namespace.method => acl
	'bar.add' => 'addbar',
	'bar.*' => 'bar',
	'prov.*' => ['provold','provnew'],
	'prov.set_tariff' => 'provjc',
	#'aps.*' => 'swh1.aps',
};

# which backend methods require filtering of which field
$backendfilter = {
	'bar.square' => 'foo',
	#'bar.square' => ['foo','foe'],
	#'bar.*' => ['_*_'], # filtering optional
};
