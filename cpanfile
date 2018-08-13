# rpc-switch requirements
# install with something like:
# cpanm --installdeps .

requires 'JSON::MaybeXS', '1.003008';

requires 'Mojolicious', '7.55';

requires 'MojoX::NetstringStream', '0.05';

recommends 'Cpanel::JSON::XS', '2.3310';
recommends 'IO::Socket::SSL', '1.94';
recommends 'Net::DNS::Native';
recommends 'RPC::Switch::Client', '0.07';
