package RPC::Switch::Auth::Base;
use Mojo::Base 'Mojo::EventEmitter';

sub authenticate {
        die "authenticate not implemented?";
}

sub request_authentication {
        die "request_authentication not implemented?";
}

1;
