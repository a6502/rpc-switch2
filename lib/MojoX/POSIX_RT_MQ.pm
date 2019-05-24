package MojoX::POSIX_RT_MQ;

use Mojo::Base 'Mojo::EventEmitter';

use Carp;
use Data::Dumper;
use Mojo::IOLoop;
use POSIX::RT::MQ qw();

our $VERSION  = '0.01';

has [qw(buf debug log mq mqfh name rdr reactor wtr)];

sub new {
	my ($class, %args)  = @_;
	my ($attr, $debug, $log, $flag, $ioloop, $mode, $name) =
		@args{qw(attr debug log flag ioloop mode name)};
	croak 'no queue name?' unless $name;
	croak 'no flags?' unless defined $flag; # may be 0
	my $self = $class->SUPER::new();
	my $mq = POSIX::RT::MQ->open($name, $flag, $mode, $attr)
		or croak "cannot open queue $name: $!";
	$mq->blocking(0);

	my $rw = $flag & 0x3; # fixme: better way to do this?
	my $rdr = $self->{rdr} = $rw != 1;
	my $wtr = $self->{wtr} = $rw != 0;

	my $mqfh = IO::Handle->new_from_fd($mq->mqdes(), '+<')
		or die "cannot fdopen mq fd: $!";

	$ioloop //= Mojo::IOLoop->singleton;

	$self->{buf} = []; # buffer for messages to be written
	$self->{debug} = $debug;
	$self->{log} = $log;
	$self->{mq} = $mq;
	$self->{mqfh} = $mqfh;
	$self->{name} = $name;
	my $reactor = $self->{reactor} = $ioloop->reactor;

	$reactor->io(
		$mqfh => sub {
                        $self->_process(@_);
        })->watch($mqfh, 0, 0);
        
	return $self;
}

sub _process {
	my ($self, $reactor, $writable) = @_;

	my $mq = $self->{mq};
	if ($writable) {
		my $buf = $self->{buf};
		my $debug = $self->{debug};
		my $log = $self->{log};
		$log->debug("$$ writable $self->{name}") if $debug;
		while ($_ = shift @$buf) {
			#unless ($mq->send(@$_)) {
			unless (POSIX::RT::MQ::mq_send($mq->{mqdes}, $$_[0], ($$_[1] || 0))) {
				unshift @$buf, $_;
				last;
			}
		}	
		unless (@$buf) {
			# stop watching if done sending
			$self->reactor->watch($self->mqfh, $self->rdr, 0);
			$log->debug("$$ emit drain $self->{name}") if $debug;
			#$log->info("$$ emit drain $self->{name} $self->{mqfh}");
			$self->emit('drain');
		}
		return;
	}

	unless ($self->has_subscribers('msg')) {
		$self->log->error("$$ $self lost subscribers!? ");
	}

	my ($msg, $prio);
	#while (($msg, $prio) = $mq->receive()) {
	while (($msg, $prio) = POSIX::RT::MQ::mq_receive($mq->{mqdes}, $mq->{_saved_attr_}{mq_msgsize})) {
		#$self->log->debug("received '$msg' with prio $prio") if $self->{debug};
		$self->emit('msg', $msg, $prio);
		#$self->log->info("$$ $self emitted 'msg'");
	}
}

sub start {
	my ($self) = @_;
	$self->log->debug("$$ start $self->{name} " . ($self->rdr ? 1 : 0) . ', ' . (@{$self->buf} ? 1 : 0));
        $self->reactor->watch($self->mqfh, $self->rdr, ($self->wtr && @{$self->buf} ? 1 : 0));
}

sub stop {
	my ($self) = @_;
        $self->reactor->watch($self->mqfh, 0, 0);
}

sub close {
	my ($self) = @_;
	$self->log->debug("close $self"); # if $self->{log};
	if ($self->{mqfh}) {
		$self->reactor->remove($self->mqfh);
		$self->mq->close;
		CORE::close($self->{mqfh});
		#say "closed $self";
		undef %$self;
	}
}

sub unlink {
	my ($self) = @_;
	$self->log->debug("unlink $self");
	if ($self->{mq}) {
		$self->{mq}->unlink;
		#say "unlinked $self";
	}
}

sub send {
	my $self = shift;

	croak "mq $self->{name} not writable" unless $self->wtr;
	my $buf = $self->{buf};
	my $debug = $self->{debug};
	my $log = $self->{log};

	if (@$buf) {
		$log->debug('queueing') if $debug;
		push @$buf, \@_;
		return 0;
	}

	#unless ($self->{mq}->send(@_)) {
	unless (POSIX::RT::MQ::mq_send($self->{mq}->{mqdes}, $_[0], ($_[1] || 0))) {
		$log->debug('send failed, queueing') if $debug;
		#$log->info("$$ send failed, queueing");
		push @$buf, \@_;
		$self->reactor->watch($self->mqfh, $self->rdr , 1);
		return 0;
	}
	#$log->debug('sent!') if $debug;
	return 1;
}

#sub DESTROY {
#	my $self = shift;
#	say "closing $self ($self->{mqfh}) in destroy";
#	close($self->{mqfh}) if $self->{mqfh};
#}

1;
