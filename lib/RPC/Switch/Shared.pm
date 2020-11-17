package RPC::Switch::Shared;

#
# shared data between all rpcswitch processes
#

# mojo (from cpan)
use Mojo::Base -strict;

# standard
use Carp qw(croak);
use Data::Dumper;
use Exporter qw(import);

# more cpan
use JSON::MaybeXS; # fixme: directly use Cpanel::Mojo::XS?
use LMDB_File qw(:cursor_op :error :flags);
use Ref::Util qw(is_arrayref is_coderef is_hashref);

use constant DEBUG => $ENV{RPC_SWITCH_SHARED_DEBUG} || 0;

our @EXPORT = qw(ins sel upd del all allforkey txn);

our %tables = (
	backend2acl => 0,
	backendfilter => 0,
	cons => 0,
	doc => 0,
	method2acl => 0,
	methods => 0,
	who2acl => 0,
	who2visacl => 0,
	wm => MDB_DUPSORT, # workers per methods
);

our %jtables = map +($_ => 1), qw(
	backend2acl
	cons
	doc
	method2acl
	methods
	who2acl
	who2visacl
	wm
);

our (
	$dbname,
	$debug,
	%dbis,
	$env,
	$log,
	$maxdbs,
	$mapsize,
	$rundir,
);
our $pid = $$;

our $MDB_DUPSORT = MDB_DUPSORT;
our $MDB_NEXT_DUP = MDB_NEXT_DUP;
our $MDB_RDONLY = MDB_RDONLY;
our $MDB_SET_KEY = MDB_SET_KEY;

# the parent calls this once
sub create_db {
	my (%args) = @_;

	for (qw(debug log rundir)) {
		die "arg $_ required" unless defined $args{$_};
	}

	($debug, $log, $maxdbs, $mapsize, $rundir) = @args{qw(debug log maxdbs mapsize rundir)};
	$maxdbs //= 1E1;
	$mapsize //= 1E6;

	$dbname = "$rundir/db";

	$env = LMDB::Env->new($dbname, {
	        flags => MDB_NOSYNC | MDB_WRITEMAP,
	        maxdbs => $maxdbs,
	        mapsize => $mapsize,
	});

	my $dbi;
	my $txn = LMDB::Txn->new($env, 0);
	while(my ($t,$f) = each %tables) {
		$dbis{$t} = $txn->open($t, $f | MDB_CREATE);
	}
	$txn->commit;
}

# all child procecesses need to call this
sub reopen_db {
	die 'use create_dB first!' unless $dbname and $env;
	
	undef %dbis;
	undef $env;

	$env = LMDB::Env->new($dbname, {
	        flags => MDB_NOSYNC | MDB_WRITEMAP,
	        maxdbs => $maxdbs,
	        mapsize => $mapsize,
	});

	my $dbi;
	my $txn = LMDB::Txn->new($env, 0);
	while(my ($t,$f) = each %tables) {
		$dbis{$t} = $txn->open($t, $f);
	}
	$txn->commit;
}

# the parent should call this once on exit
sub cleanup_db {
	return unless $pid and $pid == $$;
	say "cleaning up!" if DEBUG;
	undef $env if $env;
	if ($dbname) {
		unlink $dbname;
		unlink "$dbname-lock";
	}
}

# crud methods 

# insert
sub ins {
	my ($table, $key, $val) = @_;
	my $dbi = $dbis{$table} or croak "table $table unknown";
	croak "no key?" unless defined $key;
	$val = encode_json($val) if $jtables{$table};

	say "$$ ins $table: '$key': '$val'" if DEBUG;
	my $txn = LMDB::Txn->new($env, 0);
	$txn->put($dbi, $key, $val);
	$txn->commit;
}

# select
sub sel {
	my ($table, $key, $tx) = @_;
	my $dbi = $dbis{$table} or croak "table $table unknown";
	
	my $val;
	{
		my $txn = $tx // LMDB::Txn->new($env, $MDB_RDONLY);
	        local $LMDB_File::die_on_err = 0;
	        $txn->get($dbi, $key, $val);
		$txn->commit unless $tx;
	}
	
	#return unless defined $val;
	unless (defined $val) {
		say "$$ sel $table: $key not found" if DEBUG;
		return;
	}
	say "$$ sel $table: '$key': '$val'" if DEBUG;
	return decode_json($val) if $jtables{$table};
	return $val;
}

# update
sub upd {
	my ($table, $key, $val) = @_;
	my $dbi = $dbis{$table} or die "table $table unknown";
	croak "table $table is not a json table"  unless $jtables{$table};
	croak "val should be a hashref" unless is_hashref($val);
	
	my $oldval;
	my $txn = LMDB::Txn->new($env, 0);
	{
		local $LMDB_File::die_on_err = 0;
		$txn->get($dbi, $key, $oldval);
	}

	#return unless defined $oldval;
	unless (defined $oldval) {
		say "$$ upd $table: '$key' not found" if DEBUG;
		$txn->commit;
		return;
	}
	say "$$ upd $table: '$oldval'" if DEBUG;
	$oldval = decode_json($oldval);
	$val = encode_json({ %$oldval, %$val });
	say "$$ upd $table: '$key': oldval '$oldval' newval '$val'" if DEBUG;
	$txn->put($dbi, $key, $val);
	$txn->commit;
}

# delete
sub del {
	my ($table, $key, $val) = @_;
	my $dbi = $dbis{$table} or croak "table $table unknown";
	$val = encode_json($val) if defined $val and ref $val;

	say "$$ del $table: '$key'" if DEBUG; #, (defined $val) ? ": '$val'" : '';
	{
		local $LMDB_File::die_on_err = 0;
		my $txn = LMDB::Txn->new($env, 0);
		$txn->del($dbi, $key, (defined $val) ? $val : undef);
		$txn->commit;
	}
}

# find all values for a given key
sub allforkey {
	my ($table, $key, $json, $tx) = @_;
	my $dbi = $dbis{$table} or croak "table $table unknown";
	croak "tables $table is not MDB_DUPSORT" unless $tables{$table} & $MDB_DUPSORT;
	my $val;

	#say "allforkey $table: '$key'"; # , (defined $val) ? ": '$val'" : '';
	my @vals;
	my $txn = $tx // LMDB::Txn->new($env, $MDB_RDONLY);
	LMDB::Cursor::open($txn, $dbi, my $cursor);
	die "aargh" unless $cursor;
	local $LMDB_File::die_on_err = 0;
	my $res;
	if ($res = $cursor->_get($key, $val, $MDB_SET_KEY)) {
		# fixme: other errors?
		say "$$ allforkey $table: '$key' not found" if DEBUG; # res: $res $@";
		$cursor->close;
		$txn->commit unless $tx;
		return;
	}
	push @vals, $val;
	while ( 0 == ($res = $cursor->_get($key, $val, $MDB_NEXT_DUP)) ) {
		say "all: got '$key': '$val'" if DEBUG;
		push @vals, $val;
	}
	#say "all $res $@";
	$cursor->close;
	$txn->commit unless $tx;
	say "$$ allforkey $table: '$key' found : ", join(', ', @vals) if DEBUG;
	if ($jtables{$table} and not $json) {
		for (@vals) {
			$_ = decode_json($_);
		}
	}
	return \@vals;
}

# find all values in table
sub all {
	my ($table, $json) = @_;
	my $dbi = $dbis{$table} or croak "table $table unknown";
	my %all;
	my $txn = LMDB::Txn->new($env, MDB_RDONLY);
	LMDB::Cursor::open($txn, $dbi, my $cursor);
	die "aargh" unless $cursor;
	local $LMDB_File::die_on_err = 0;
	my ($res, $key, $val);
	if ($res = $cursor->_get($key, $val, MDB_FIRST)) {
		say "res: $res $@" if DEBUG;
		$txn->commit;
		return;
	}
	$all{$key} = $val;
	while ( 0 == ($res = $cursor->_get($key, $val, MDB_NEXT)) ) {
		say "all: got '$key': '$val'" if DEBUG;
		$all{$key} = $val;
	}
	#say "all $res $@"; # will be -30798 MDB_NOTFOUND: No matching key/data pair found
	say "$$ all $table found: ", join(', ', map { "$_ => $all{$_}" } keys %all) if DEBUG;
	$txn->commit;
	if ($jtables{$table} and not $json) {
		for (values %all) {
			$_ = decode_json($_);
		}
	}
	return \%all;
}

sub txn {
	return LMDB::Txn->new($env, MDB_RDONLY);	
}
