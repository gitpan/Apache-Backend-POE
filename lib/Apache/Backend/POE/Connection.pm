package Apache::Backend::POE::Connection;

use warnings;
use strict;

use Apache::Backend::POE::Message;
use IO::Socket::INET;
use Carp qw(croak);
use bytes;
use Storable qw(nfreeze thaw);

sub new {
	my $class = shift;
	return bless({
		@_, # can be a hash
	}, $class);
}

sub connect {
 	my ($obj, $poe, $host, $port) = @_;

	my $self = bless({%{$obj}},ref($obj));
	$self->{parent} = $poe;
	$self->{service_name} = $self->{alias} || 'backend-';

	if ($host && $port) {
		$self->{host} = $host;
		$self->{port} = $port;
	}

	unless ($self->{host} && $self->{port}) {
		croak "Must pass host and port to connect or initial new";
	}

	# connect

	$self->{socket} = IO::Socket::INET->new(
		PeerAddr => $self->{host},
		PeerPort => $self->{port},
	);

	croak "Couldn't connect to $self->{host} : $self->{port} - $!" unless defined $self->{socket};
	binmode($self->{socket});
	$self->{socket}->autoflush();

	$self->{buffer} = "";
	$self->{read_length} = undef;

	# register

	$self->msg_send($self->msg({
        cmd => "register_service",
        svc_name => $self->{service_name}.$$,
	}));

	return $self;
}

sub msg {
	my $self = shift;
	return Apache::Backend::POE::Message->new(@_);
}

sub ping {
	my $self = shift;
	my $start = time();
#	my $no_pong = 1;
	
	$self->msg_send($self->msg({
			cmd => 'ping',
			time => time(),
	}));

	my $msg = $self->msg_read();
	unless (ref($msg)) {
		print STDERR "received a non reference\n" if $Apache::Backend::POE::DEBUG;
		return -1;
	}
	
	if ($Apache::Backend::POE::DEBUG) {
#		print STDERR "got a ".ref($msg)." package with no event()\n",return -99 unless ($msg->can('event'));
	}
	
	my $function = $msg->event();
#	print STDERR "function: $function\n";
	if ($Apache::Backend::POE::DEBUG) {
		print STDERR "no function in message\n" unless defined $function;
	}
	return 1 if (defined $function && $function eq 'pong');
  
	print STDERR "WRONG function received: $function\n" if $Apache::Backend::POE::DEBUG;
	
	return 0;
}

sub disconnect {
	my $self = shift;
	close($self->{socket}) if ($self->{socket});
	$self->{socket} = undef;
}

sub msg_read {
	my $self = shift;
	
	while (1) {
		if (defined $self->{read_length}) {
			if (length($self->{buffer}) >= $self->{read_length}) {
				my $message = thaw(substr($self->{buffer}, 0, $self->{read_length}, ""));
				$self->{read_length} = undef;
				$message->{recv_time} = time();
				return $message;
			}
		} elsif ($self->{buffer} =~ s/^(\d+)\0//) {
			$self->{read_length} = $1;
			next;
		}
	
		my $octets_read = sysread($self->{socket}, $self->{buffer}, 4096, length($self->{buffer}));
		return unless $octets_read;
	}

}

sub msg_send {
	my $self = shift;
	my $message = shift;
	$message->{send_time} = time();
	my $streamable = nfreeze($message);

	$streamable = length($streamable).chr(0).$streamable;
	my $len = length($streamable);
	while ($len > 0) {
		if (my $w = syswrite($self->{socket},$streamable,4096)) {
			$len -= $w;
		} else {
			last;
		}
	}
	#print *{$self->{socket}} $streamable;
}

sub msg_oneshot {
	my ($obj, $host, $port, $message) = @_;

	my $self = $obj->connect(undef, $host, $port);

	my $msg = $self->msg($message);
	
	$self->msg_send($msg);

	$self->disconnect();
}

1;
