#!/usr/bin/perl

use strict;
use warnings 'all';

use DateTime;
use DateTime::Duration;
use DateTime::Format::ISO8601;
use DateTime::Format::Duration::ISO8601;

# Next two methods are needed to properly serialize the dates and durations

sub DateTime::Duration::TO_JSON {
	my $self = shift;
	
	my $format = DateTime::Format::Duration::ISO8601->new();
	
	return $format->format_duration($self);
}

sub DateTime::TO_JSON {
	my $self = shift;
	
	return $self->datetime().'Z';
}

package RepoMatcher::Abstract;

use Carp;
use Scalar::Util qw(blessed);
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;
use Time::HiRes qw();

sub new($;$) {
	my $self = shift;
	my $class = ref($self) || $self;
	
	$self = {}  unless(ref($self));

	my($config,$ua) = @_;
	
	# It should be a Config::IniFiles instance
	Carp::croak('Expected a Config::IniFiles instance as first parameter')  unless(blessed($config) && $config->isa('Config::IniFiles'));
	$self->{'config'} = $config;
	
	$self->{'ua'} = (blessed($ua) && $ua->isa('LWP::UserAgent')) ? $ua : LWP::UserAgent->new();
	
	$self->{'jp'} = JSON::MaybeXS->new('convert_blessed' => 1);
	
	return bless($self,$class);
}

sub reqPeriod() {
	my $self = shift;
	
	unless(defined($self->{'req-period'})) {
		my $config = $self->{'config'};
		my $numreq = $config->val($self->kind(),'numreq',$config->val('default','numreq',3600));
		$self->{'req-period'} = 3600 / $numreq;
	}
	
	return $self->{'req-period'};
}

sub kind() {
	Carp::croak('Unimplemented method');
}

sub doesMatch($) {
	Carp::croak('Unimplemented method');
}

sub getRepoData(\%) {
	Carp::croak('Unimplemented method');
}

# Shared method to fetch data from repos
sub fetchJSON($;$$$$) {
	my $self = shift;
	
	my($bUri,$user,$token,$p_acceptHeaders,$numIter) = @_;
	
	my $ua = $self->{'ua'};
	$numIter = 0  unless(defined($numIter));
	
	my $uriStr = (blessed($bUri) && $bUri->can('as_string')) ? $bUri->as_string() : $bUri;
	my $response;
	my $bData = undef;
	my $j = $self->{'jp'};
	my $period = $self->reqPeriod();
	
	do {
		my $bUriStr = $uriStr;
		$uriStr = undef;
		
		my $req = HTTP::Request->new('GET' => $bUriStr);
		if(defined($p_acceptHeaders)) {
			my $headers = $req->headers();
			$headers->push_header(Accept => $p_acceptHeaders);
		}
		
		$req->authorization_basic($user,$token)  if(defined($user));
		
		# To honor the limit of 5000 requests per hour
		my $t0 = Time::HiRes::gettimeofday();
		$response = $ua->request($req);
		
		if($response->is_success()) {
			eval {
				my $newBData = $j->decode($response->decoded_content);
				#$bData = JSON->new()->utf8(1)->decode($response->decoded_content);
				
				# Assuming it is an array
				if(defined($bData)) {
					push(@{$bData},@{$newBData});
				} else {
					$bData = $newBData;
				}
			};
			
			if($@) {
				print STDERR "ERROR: JSON parsing error on $bUriStr: $@\n";
			} else {
				my $linkH = $response->header('Link');
				
				# Are we paginating?
				if(defined($linkH) && length($linkH) > 0) {
					foreach my $link (split(/, /,$linkH)) {
						my($newLink,$newRel) = split(/; /,$link);
						
						if($newRel eq "rel='next'") {
							$newLink =~ tr/\<\>//d;
							$uriStr = $newLink;
							$numIter --;
							last;
						}
					}
				}
			}
		} else {
			print STDERR "ERROR: kicked out $bUriStr : ".$response->status_line()."\n".$req->as_string()."\n".$response->as_string()."\n";
		}
		
		# Should we sleep?
		my $leap = Time::HiRes::gettimeofday() - $t0;
		Time::HiRes::sleep($period-$leap)  if($period > $leap);
	} while(defined($uriStr) && $numIter != 0);
	
	return ($response->is_success(),$bData);
}

1;
