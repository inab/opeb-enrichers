#!/usr/bin/perl

use strict;
use warnings 'all';

package RepoMatcher::GitHub;

use base qw(RepoMatcher::Abstract);

use Carp;
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;

use DateTime::Duration;
use DateTime::Format::ISO8601;

use Scalar::Util qw(blessed);

# Constants

use constant GITHUB_KIND => 'github';

use constant GITHUB_HOST => 'github.com';
use constant GITHUB_IO_HOST => 'github.io';

sub new($;$) {
	my $self = shift;
	my $class = ref($self) || $self;
	
	$self = $class->SUPER::new(@_)  unless(ref($self));
	
	# It is case insensitive
	$self->{'githubRepoDataCache'} = {};
	
	$self->{'githubUserCache'} = {};
	
	$self->{'isoparser'} = DateTime::Format::ISO8601->new();
	
	return bless($self,$class);
}


sub kind() {
	return GITHUB_KIND;
}

my $GITHUB_PATTERN = ':\/\/'.GITHUB_HOST.'\/([^"\'\/]+)\/([^"\'\/]+)';

my $GITHUB_COMPILED_PATTERN = qr/$GITHUB_PATTERN/;

sub doesMatch($) {
	my $self = shift;
	
	my($uriStr) = @_;
	
	my $ua = $self->{'ua'};
	
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
							($isUri,$owner,$repo) = $self->doesMatch($ghURI->query());
							return ($isUri,$owner,$repo);
						} elsif(length($path[0]) > 0) {
							$repo = $path[0];
						}
					}
					
					unless(defined($repo)) {
						# It is some kind of web
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


use constant GITHUB_ENDPOINT => 'https://'.GITHUB_HOST;
use constant GITHUB_API_ENDPOINT => 'https://api.github.com/';

use constant GITHUB_API_V_HEADER => 'application/vnd.github.v3+json';

sub getGitHubUser($) {
	my $self = shift;
	
	my($username) = @_;
	my $p_gitHubUserCache = $self->{'githubUserCache'};
	
	if(exists($p_gitHubUserCache->{$username})) {
		return (1,$p_gitHubUserCache->{$username});
	} else {
		my $ua = $self->{'ua'};
		my $config = $self->{'config'};
		my $user = $config->val(GITHUB_KIND,'gh-user');
		my $token = $config->val(GITHUB_KIND,'gh-token');
		
		my $userUri = URI->new(GITHUB_API_ENDPOINT);
		$userUri->path_segments("","users",$username);
		
		my($userSuccess,$userData) = $self->fetchJSON($userUri,$user,$token,GITHUB_API_V_HEADER);
		
		$p_gitHubUserCache->{$username} = $userData  if($userSuccess);
		
		return ($userSuccess,$userData);
	}
}


my %recognizedBuildSystemsByLang = (
	'Makefile'	=>	'make',
	'CMake'	=>	'cmake',
);

my %recognizedInterpretedLanguages = (
	'python'	=> undef,
	'perl'	=>	undef,
	'ruby'	=>	undef,
	'r'	=>	undef,
	'php'	=>	undef,
	'golang'	=>	undef,
	'javascript'	=>	undef,
	'shell'	=>	undef,
	'jsoniq'	=>	undef
);

my %recognizedCompiledLanguages = (
	'c'	=> undef,
	'c++'	=> undef,
	'java'	=> undef,
	'fortran'	=> undef,
	'perl 6'	=>	undef,
	'pascal'	=>	undef,
	'objective-c'	=>	undef,
	'component pascal'	=>	undef,
	'scala'	=>	undef,
);

sub getRepoData(\%) {
	my $self = shift;
	
	my($fullrepo) = @_;
	
	my($owner,$repo) = @{$fullrepo}{('workspace','repo')};
	my $ua = $self->{'ua'};
	
	my $lcOwner = lc($owner);
	my $lcRepo = lc($repo);
	
	my $p_githubRepoDataCache = $self->{'githubRepoDataCache'};
	
	$p_githubRepoDataCache->{$lcOwner} = {}  unless(exists($p_githubRepoDataCache->{$lcOwner}));
	
	unless(exists($p_githubRepoDataCache->{$lcOwner}{$lcRepo})) {
		my $config = $self->{'config'};
		my $gh_user = $config->val(GITHUB_KIND,'gh-user');
		my $gh_token = $config->val(GITHUB_KIND,'gh-token');
		
		my %ans = ();
		
		# These are all the URIs the program needs to fetch from GitHub
		
		print STDERR " * Processing $owner $repo\n";
		my $repoUri = URI->new(GITHUB_API_ENDPOINT);
		$repoUri->path_segments("","repos",$owner,$repo);
		my($repoSuccess,$repoData) = $self->fetchJSON($repoUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
		
		my $minProcessing = !$repoSuccess;
		
		if($repoSuccess) {
			print STDERR "\t- Extended processing $owner $repo\n";
			# The real repo and/or owner could be different from the registered one
			my $realOwner;
			my $lcRealOwner;
			my $realRepo;
			my $lcRealRepo;
			my $followProcessing = 1;
			
			if(exists($repoData->{'owner'}) && exists($repoData->{'owner'}{'login'})) {
				$realOwner = $repoData->{'owner'}{'login'};
				$lcRealOwner = lc($realOwner);
				
				$p_githubRepoDataCache->{$lcRealOwner} = {}  unless(exists($p_githubRepoDataCache->{$lcRealOwner}));
				
				if(exists($repoData->{'name'})) {
					$realRepo = $repoData->{'name'};
					$lcRealRepo = lc($realRepo);
				}
			}
			
			if(defined($realRepo)) {
				if($lcRealOwner ne $lcOwner || $lcRealRepo ne $lcRepo) {
					if(exists($p_githubRepoDataCache->{$lcRealOwner}{$lcRealRepo})) {
						# Nothing more to do, work previously done
						$p_githubRepoDataCache->{$lcOwner}{$lcRepo} = $p_githubRepoDataCache->{$lcRealOwner}{$lcRealRepo};
						
						$followProcessing = undef;
					} else {
						# Two for one bargain!
						$p_githubRepoDataCache->{$lcRealOwner}{$lcRealRepo} =  \%ans;
						$p_githubRepoDataCache->{$lcOwner}{$lcRepo} = \%ans;
					}
				} else {
					$p_githubRepoDataCache->{$lcRealOwner}{$lcRealRepo} =  \%ans;
				}
			} else {
				# Nothing more to do here, min processing
				$followProcessing = undef;
				$minProcessing = 1;
			}
			
			if($followProcessing) {
				if(exists($repoData->{'clone_url'}) && defined($repoData->{'clone_url'})) {
					$ans{'owner'} = $realOwner;
					$ans{'workspace'} = $realOwner;
					$ans{'repo'} = $realRepo;
					
					$ans{'numForks'} = $repoData->{'forks_count'};
					$ans{'numWatchers'} = $repoData->{'watchers_count'};
					$ans{'creationDate'} = $repoData->{'created_at'};
					$ans{'lastUpdate'} = $repoData->{'updated_at'};
					$ans{'lastPush'} = $repoData->{'pushed_at'};
					
					$ans{'vcs_type'} = 'git';
					# sourcecode repository
					$ans{'vcs_uri'} = $repoData->{'clone_url'};
					# sourcecode
					$ans{'source_uri'} = $repoData->{'clone_url'};
					# sourcecode download registration
					$ans{'source_isDownloadRegistered'} = JSON->false;
					# sourcecode anonymous access
					$ans{'source_hasAnonymousAccess'} = JSON->true;
					# sourcecode repository browse
					$ans{'source_isRepoBrowsable'} = JSON->true;
					# Source_code repository version controlled
					$ans{'source_hasVcsRepo'} = JSON->true;
					
					# TO IMPROVE
					# Source code repository accessible
					$ans{'source_isRepoAccessible'} = JSON->true;
				}
				
				# website
				if(exists($repoData->{'homepage'}) && defined($repoData->{'homepage'}) && length($repoData->{'homepage'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'homepage'};
				} elsif(exists($repoData->{'html_url'}) && defined($repoData->{'html_url'}) && length($repoData->{'html_url'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'html_url'};
				}
				
				if(exists($repoData->{'description'}) && defined($repoData->{'description'})) {
					# canonical:description
					$ans{'desc'} = $repoData->{'description'};
					# canonical:concept
					$ans{'concept'} = $repoData->{'description'};
				}
				
				# canonical|version:HELP
				$ans{'has_help'} = ((exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'}) || (exists($repoData->{'has_pages'}) && $repoData->{'has_pages'}))? JSON->true : JSON->false;
				# canonical|version:Tutorial
				$ans{'has_tutorial'} = ((exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'}) || (exists($repoData->{'has_pages'}) && $repoData->{'has_pages'})) ? JSON->true : JSON->false;
				
				if($repoData->{'has_issues'}) {
					# issue tracker
					$ans{'has_issue_tracker'} = JSON->true;

					my $issuesUri = URI->new(GITHUB_API_ENDPOINT);
					$issuesUri->path_segments("","repos",$realOwner,$realRepo,'issues');
					$issuesUri->query_form('state' => 'all');
					my($issuesSuccess,$issuesData) = $self->fetchJSON($issuesUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					
					# It gathers the date of the last issue, and the date of the last update
					if($issuesSuccess) {
						
						my $numOpen = 0;
						my $numClosed = 0;
						my $lastClosed = undef;
						my $lastUpdated = undef;
						my $lastCreated = undef;
						
						my $minDuration = undef;
						my $maxDuration = undef;
						my $totalDuration = DateTime::Duration->new('seconds' => 0);
						
						my $isoparser = $self->{'isoparser'};
						
						foreach my $issue (@{$issuesData}) {
							if($issue->{'state'} eq 'closed') {
								$numClosed++;
								my $closedDt = $isoparser->parse_datetime($issue->{'closed_at'});
								my $createdDt = $isoparser->parse_datetime($issue->{'created_at'});
								my $duration = $closedDt - $createdDt;
								my $absDuration = $closedDt->subtract_datetime_absolute($createdDt);
								
								$totalDuration = $totalDuration + $absDuration;
								if(!defined($minDuration) || DateTime::Duration->compare($minDuration,$duration) > 0) {
									$minDuration = $duration;
								}
								if(!defined($maxDuration) || DateTime::Duration->compare($duration,$maxDuration) > 0) {
									$maxDuration = $duration;
								}
							} else {
								$numOpen++;
							}
							
							$lastCreated = $issue->{'created_at'}  if(defined($issue->{'created_at'}) && (!defined($lastCreated) || $lastCreated lt $issue->{'created_at'}));
							$lastUpdated = $issue->{'updated_at'}  if(defined($issue->{'updated_at'}) && (!defined($lastUpdated) || $lastUpdated lt $issue->{'updated_at'}));
							$lastClosed = $issue->{'closed_at'}  if(defined($issue->{'closed_at'}) && (!defined($lastClosed) || $lastClosed lt $issue->{'closed_at'}));
						}
						
						my %issues = (
							'total' => scalar(@{$issuesData}),
							'open'	=>	$numOpen,
							'closed'	=> $numClosed,
							'lastClosedDate'	=>	$lastClosed,
							'lastUpdatedDate'	=>	$lastUpdated,
							'lastCreatedDate'	=>	$lastCreated
						);
						if(defined($minDuration)) {
							$issues{'minDuration'} = $minDuration;
							$issues{'maxDuration'} = $maxDuration;
							
							my $totalSeconds = $totalDuration->in_units('seconds');
							my $meanSeconds = int($totalSeconds/$numClosed + 0.5);
							$issues{'meanDuration'} = DateTime::Duration->new('seconds' => $meanSeconds);
						}
						$ans{'issues'} = \%issues;
					}
				}
				
				# All the gathered version tags go to this array
				my @versions = ();
				
				my $releasesUri = URI->new(GITHUB_API_ENDPOINT);
				$releasesUri->path_segments("","repos",$realOwner,$realRepo,'releases');
				my($releasesSuccess,$releasesData) = $self->fetchJSON($releasesUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
				if($releasesSuccess) {
					# Versions
					foreach my $release (@{$releasesData}) {
						push(@versions,$release->{'tag_name'});
						
						if(exists($release->{'assets'})) {
							$ans{'binaries'} = []  unless(exists($ans{'binaries'}));
							foreach my $asset (@{$release->{'assets'}}) {
								my $p_b = {
									'creationDate' => $asset->{'created_at'},
									'numDownloads' => $asset->{'download_count'},
									'binary_uri' => $asset->{'browser_download_url'},
									'binary_isDistributable' => JSON->true,
									'binary_isDownloadRegistered' => JSON->false,
								};
								# binary distribution
								unless(exists($ans{'binary_uri'})) {
									$ans{'binary_uri'} = $asset->{'browser_download_url'};
									# binary distribution freeness
									$ans{'binary_isDistributable'} = JSON->true;
									# binaries download registration
									$ans{'binary_isDownloadRegistered'} = JSON->false;
								}
								push(@{$ans{'binaries'}},$p_b);
							}
						}
					}
				}
				
				
				my $tagsUri = URI->new(GITHUB_API_ENDPOINT);
				$tagsUri->path_segments("","repos",$realOwner,$realRepo,'tags');
				my($tagsSuccess,$tagsData) = $self->fetchJSON($tagsUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
				if($tagsSuccess) {
					# Tags treated as versions
					my %vercheck = map { $_ => undef } @versions;
					foreach my $tag (@{$tagsData}) {
						push(@versions,$tag->{'name'})  unless(exists($vercheck{$tag->{'name'}}));
					}
				}
				
				my $langsUri = URI->new(GITHUB_API_ENDPOINT);
				$langsUri->path_segments("","repos",$realOwner,$realRepo,'languages');
				my($langsSuccess,$langsData) = $self->fetchJSON($langsUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
				if($langsSuccess) {
					$ans{'languages'} = [ keys(%{$langsData}) ];
					
					my $interpreted = JSON->false;
					my $compiled = JSON->false;
					foreach my $lang (keys(%{$langsData})) {
						my $lcLang = lc($lang);
						if(exists($recognizedInterpretedLanguages{$lcLang})) {
							$interpreted = JSON->true;
						}
						if(exists($recognizedCompiledLanguages{$lcLang})) {
							$compiled = JSON->true;
						}
					}
					$ans{'has_interpretedLanguages'} = $interpreted;
					$ans{'has_compiledLanguages'} = $compiled;
					
					# 'type'
					foreach my $lang (keys(%recognizedBuildSystemsByLang)) {
						if(exists($langsData->{$lang})) {
							$ans{'tool_buildSystem'} = $recognizedBuildSystemsByLang{$lang};
							last;
						}
					}
					
				}
				
				my @contributors = ();
				
				my $contrUri = URI->new(GITHUB_API_ENDPOINT);
				$contrUri->path_segments("","repos",$realOwner,$realRepo,'contributors');
				my($contrSuccess,$contrData) = $self->fetchJSON($contrUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
				if($contrSuccess) {
					# credits
					foreach my $contributor (@{$contrData}) {
						my $p_contrib = $self->getGitHubUser($contributor->{'login'});

						my %contribOpEB = (
							'username'	=> $contributor->{'login'},
						);
						
						for my $key ( ('name','company','location','email') ) {
							if(exists($p_contrib->{$key}) && defined($p_contrib->{$key})) {
								$contribOpEB{$key} = $p_contrib->{$key};
							}
						}
						push(@contributors,\%contribOpEB);
					}
				}
				
				
				# sourcecode freeness. The default license is the owner itself
				$ans{'source_isFree'} = JSON->false;
				$ans{'has_license'} = defined($repoData->{'license'}) ? JSON->true : JSON->false;
				
				if($ans{'has_license'}) {
					my $licUri = URI->new(GITHUB_API_ENDPOINT);
					$licUri->path_segments("","repos",$realOwner,$realRepo,'license');
					my($licSuccess,$licData) = $self->fetchJSON($licUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					if($licSuccess) {
						# sourcecode freeness
						$ans{'source_isFree'} = JSON->true;
						if(exists($licData->{'_links'}{'html'})) {
							# project license
							$ans{'tool_license_uri'} = $licData->{'_links'}{'html'};
							# sourcecode license
							$ans{'source_license_uri'} = $licData->{'_links'}{'html'};
						}
						
						if(exists($licData->{'license'}{'spdx_id'}) && defined($licData->{'license'}{'spdx_id'})) {
							$ans{'source_license'} = $licData->{'license'}{'spdx_id'};
						} elsif(exists($licData->{'license'}{'key'}) && defined($licData->{'license'}{'key'})) {
							$ans{'source_license'} = $licData->{'license'}{'key'};
						} elsif(exists($licData->{'license'}{'name'}) && defined($licData->{'license'}{'name'})) {
							$ans{'source_license'} = $licData->{'license'}{'name'};
						}
						
						# TO IMPROVE
						if($licData->{'license'}{'featured'}) {
							# 'open source'
							$ans{'is_opensource'} = JSON->true;
							# 'OSI'
							$ans{'is_OSI'} = JSON->true;
						}
					}
				}
				
				# Looking for a README and other elements
				my $commitUri = URI->new(GITHUB_API_ENDPOINT);
				$commitUri->path_segments("","repos",$realOwner,$realRepo,'commits');
				my($commitSuccess,$commitData) = $self->fetchJSON($commitUri,$gh_user,$gh_token,GITHUB_API_V_HEADER,1);
				if($commitSuccess && scalar(@{$commitData}) > 0) {
					my $treeUri = $commitData->[0]{'commit'}{'tree'}{'url'};
					
					my($treeSuccess,$treeData) = $self->fetchJSON($treeUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					if($treeSuccess) {
						foreach my $elem (@{$treeData->{'tree'}}) {
							if(index(lc($elem->{'path'}),'readme') == 0) {
								$ans{'readmeFile'} = $elem->{'path'};
								last;
							}
						}
					}
				}
				
				#my $pullsUri = URI->new(GITHUB_API_ENDPOINT);
				#$pullsUri->path_segments("","repos",$realOwner,$realRepo,'pulls');
				#my($pullsSuccess,$pullsData) = $self->fetchJSON($pullsUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
				#if($pullsSuccess) {
				#	use Data::Dumper;
				#	print Dumper($pullsData),"\n";
				#}
				#
				#
				
				# Versions
				$ans{'tool_versions'} = \@versions  if(scalar(@versions) > 0);
				# credits
				$ans{'tool_developers'} = \@contributors  if(scalar(@contributors) > 0);
			}
		}
		
		# These are failed, strange cases
		if($minProcessing) {
			$p_githubRepoDataCache->{$lcOwner}{$lcRepo} = \%ans;
			
			# Alternate way
			my $clone_uri = URI->new(GITHUB_ENDPOINT);
			$clone_uri->path_segments('',$owner,$repo);
			$ans{'vcs_type'} = 'git';
			# sourcecode repository
			$ans{'vcs_uri'} = $clone_uri->as_string();
			
			$ans{'owner'} = $owner;
			$ans{'workspace'} = $owner;
			$ans{'repo'} = $repo;
		}
	}
	
	return $p_githubRepoDataCache->{$lcOwner}{$lcRepo};
}	

1;
