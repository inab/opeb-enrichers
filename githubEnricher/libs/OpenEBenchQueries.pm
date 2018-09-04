#!/usr/bin/perl

use strict;
use warnings 'all';

use Carp;
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;

package OpenEBenchQueries;

use Scalar::Util qw(blessed);

use constant OPENEBENCH_SOURCE	=>	'https://openebench.bsc.es/monitor/rest/search';
use constant GITHUB_HOST => 'github.com';
use constant GITHUB_IO_HOST => 'github.io';


sub new(;$$) {
	my $self = shift;
	my $class = ref($self) || $self;
	
	$self = {}  unless(ref($self));
	
	my($load_opeb_filename,$save_opeb_filename) = @_;
	
	$self->{'load_opeb_filename'} = $load_opeb_filename;
	$self->{'save_opeb_filename'} = $save_opeb_filename;
	
	return bless($self,$class);
}

sub extractGitHubIds(;$$) {
	my $self = shift;
	
	my($sourceURL,$ua) = @_;
	
	$sourceURL = OPENEBENCH_SOURCE  unless(defined($sourceURL));
	
	my $raw_opeb;
	if(defined($self->{'load_opeb_filename'})) {
		if(open(my $OP,'<:encoding(UTF-8)',$self->{'load_opeb_filename'})) {
			undef $/;
			$raw_opeb = <$OP>;
			close($OP);
		} else {
			Carp::croak("Error reading input OpenEBench payload ".$self->{'load_opeb_filename'}." . Reason: $!");
		}
	} else {
		$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
		my $req = HTTP::Request->new('GET' => OPENEBENCH_SOURCE);
		
		my $response = $ua->request($req);
		
		my $retval = undef;
		if($response->is_success()) {
			$raw_opeb = $response->decoded_content();
		} else {
			Carp::croak("ERROR: kicked out: ".$response->status_line());
		}
	}
	
	# Saving the payload
	if(defined($self->{'save_opeb_filename'})) {
		if(open(my $OP,'>:encoding(UTF-8)',$self->{'save_opeb_filename'})) {
			print $OP $raw_opeb;
			
			close($OP);
		} else {
			Carp::croak("Error saving input OpenEBench payload ".$self->{'save_opeb_filename'}." . Reason: $!");
		}
	}
	
	# Now, parse it 
	my $retval = JSON::MaybeXS->new()->decode($raw_opeb);

	if($@) {
		Carp::croak("ERROR: JSON parsing error: $@");
	}
	
	return $self->parseOpenEBench($retval);
}

my %Features = (
	'documentation'	=>	{
		'general'	=>	undef,
		'manual'	=>	undef
	},
	'distributions' => {
		'source_packages'	=>	undef,
		'binary_packages'	=>	undef,
		'sourcecode'	=>	undef,
		'binaries'	=>	undef
	},
	'homepage'	=>	undef,
	'repositories'	=>	undef
);

sub _linkExtract(\%\%);

sub _linkExtract(\%\%) {
	my($p_entry,$p_features) = @_;
	
	my @entry_links = ();
	
	foreach my $mkey (keys(%{$p_features})) {
		if(exists($p_entry->{$mkey}) && defined($p_entry->{$mkey})) {
			my $s_features = $p_features->{$mkey};
			
			if(defined($s_features)) {
				my $s_entry = $p_entry->{$mkey};
				push(@entry_links,_linkExtract(%{$s_entry},%{$s_features}));
			} elsif(ref($p_entry->{$mkey}) eq 'ARRAY') {
				foreach my $it (@{$p_entry->{$mkey}}) {
					push(@entry_links,$it)  if(defined($it) && length($it) > 0);
				}
			} elsif(length($p_entry->{$mkey}) > 0) {
				push(@entry_links,$p_entry->{$mkey});
			}
		}
	}
	
	return @entry_links;
}


my $GITHUB_PATTERN = ':\/\/'.GITHUB_HOST.'\/([^"\'\/]+)\/([^"\'\/]+)';

my $GITHUB_COMPILED_PATTERN = qr/$GITHUB_PATTERN/;

sub _matchGitHub($;$);

sub _matchGitHub($;$) {
	my($uriStr,$ua) = @_;
	
	my $isUri = undef;
	my $owner = undef;
	my $repo = undef;
	
	# Is an URI from GitHub?
	eval {
		my $ghURI = URI->new($uriStr);
		if(defined($ghURI->scheme())) {
			$isUri=1;
			if(index($ghURI->scheme(), 'http') == 0 || index($ghURI->scheme(), 'git') == 0) {
				my $host = $ghURI->host();
				my @path = $ghURI->path_segments();
				shift(@path)  if(scalar(@path) > 0 && $path[0] eq '');
				
				if($host eq GITHUB_HOST) {
					if(scalar(@path)>=2) {
						($owner,$repo) = @path[0,1];
						
						# If it ends in \.git
						$repo = substr($repo,0,-4)  if($repo =~ /.git$/);
					}
				} elsif(substr($host,-length(GITHUB_IO_HOST)) eq GITHUB_IO_HOST) {
					if(scalar(@path) >= 1) {
						$owner = substr($host,0,index($host,'.'));
						
						if($owner eq 'htmlpreview') {
							($isUri,$owner,$repo) = _matchGitHub($ghURI->query(),$ua);
							return ($isUri,$owner,$repo);
						} elsif(length($path[0]) > 0) {
							$repo = $path[0];
						}
					}
					
					unless(defined($repo)) {
						# It is some kind of web
						$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
						
						my $ghURIStr = $ghURI->as_string();
						my $req = HTTP::Request->new('GET' => $ghURIStr);
						my $response = $ua->request($req);
						if($response->is_success()) {
							my $page = $response->decoded_content();
							while($page =~ /$GITHUB_COMPILED_PATTERN/g) {
								$owner = $1;
								$repo = $2;
								last;
							}
						}
					}
				}
			}
		}
	};
	
	return ($isUri,$owner,$repo);
}


sub parseOpenEBench(\@;$) {
	my $self = shift;
	
	my($p_entries,$ua) = @_;
	
	my @queries = ();
	
	$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
	foreach my $entry (@{$p_entries}) {
		my @entry_links = _linkExtract(%{$entry},%Features);
		
		if(scalar(@entry_links) > 0) {
			my %gitHubEntries = ();
			my @repos = ();
			
			foreach my $entry_link (@entry_links) {
				my($isURI,$owner,$repo) = _matchGitHub($entry_link,$ua);
				
				if($isURI && defined($owner) && (length($owner) > 0) && defined($repo) && (length($repo) > 0)) {
					$gitHubEntries{$owner} = {}  unless(exists($gitHubEntries{$owner}));
					unless(exists($gitHubEntries{$owner}{$repo})) {
						my $p_links = [];
						$gitHubEntries{$owner}{$repo} = $p_links;
						push(@repos,{
							'owner'	=>	$owner,
							'repo'	=>	$repo,
							'links'	=>	$p_links
						});
					}
					
					push(@{$gitHubEntries{$owner}{$repo}},$entry_link);
				}
			}
			
			# Return only those ones with something interesting
			if(scalar(@repos) > 0) {
				push(@queries,{
					'@id' =>	$entry->{'@id'},
					'entry_links'	=>	\@entry_links,
					'repos'	=>	\@repos
				});
			}
		}
	}
	
	return \@queries;
}

1;
