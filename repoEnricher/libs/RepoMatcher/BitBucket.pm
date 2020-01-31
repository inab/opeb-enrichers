#!/usr/bin/perl

use strict;
use warnings 'all';

package RepoMatcher::BitBucket;

use base qw(RepoMatcher::Abstract);

use Carp;
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;

use DateTime::Duration;
use DateTime::Format::ISO8601;

use Scalar::Util qw(blessed);



# Documentation: https://developer.atlassian.com/server/bitbucket/reference/rest-api/

# Constants

use constant BITBUCKET_KIND => 'bitbucket';

use constant BITBUCKET_HOST => 'bitbucket.org';
use constant BITBUCKET_IO_HOST => 'bitbucket.io';
use constant BITBUCKET_PAGELEN => 'pagelen';


sub new($;$) {
	my $self = shift;
	my $class = ref($self) || $self;
	
	$self = $class->SUPER::new(@_)  unless(ref($self));
	
	# It is case insensitive
	$self->{'bitbucketRepoDataCache'} = {};
	
	$self->{'bitbucketUserCache'} = {};
	
	$self->{'isoparser'} = DateTime::Format::ISO8601->new();
	
	return bless($self,$class);
}


sub kind() {
	return BITBUCKET_KIND;
}

my $BITBUCKET_PATTERN = ':\/\/(?:[^@\/]+@)?'.BITBUCKET_HOST.'\/([^"\'\/]+)\/([^"\'\/]+)(?:\.git)?';

my $BITBUCKET_COMPILED_PATTERN = qr/$BITBUCKET_PATTERN/;

sub doesMatch($) {
	my $self = shift;
	
	my($uriStr) = @_;
	
	my $ua = $self->{'ua'};
	
	my $isUri = undef;
	my $workspace = undef;
	my $repoSlug = undef;
	
	# Is an URI from BitBucket?
	eval {
		my $bbURI = URI->new($uriStr);
		if(defined($bbURI->scheme())) {
			$isUri=1;
			if(index($bbURI->scheme(), 'http') == 0) {
				my $host = $bbURI->host();
				my @path = $bbURI->path_segments();
				shift(@path)  if(scalar(@path) > 0 && $path[0] eq '');
				
				if($host eq BITBUCKET_HOST) {
					if(scalar(@path)>=2) {
						($workspace,$repoSlug) = @path[0,1];
						
						# If it ends in \.git
						$repoSlug = substr($repoSlug,0,-4)  if($repoSlug =~ /.git$/);
					}
				} elsif(substr($host,-length(BITBUCKET_IO_HOST)) eq BITBUCKET_IO_HOST) {
					if(scalar(@path) >= 1) {
						$workspace = substr($host,0,index($host,'.'));
						
						if($workspace eq 'htmlpreview') {
							($isUri,$workspace,$repoSlug) = $self->doesMatch($bbURI->query());
							return ($isUri,$workspace,$repoSlug);
						} elsif(length($path[0]) > 0) {
							$repoSlug = $path[0];
						}
					}
					
					unless(defined($repoSlug)) {
						# It is some kind of web
						my $bbURIStr = $bbURI->as_string();
						my $req = HTTP::Request->new('GET' => $bbURIStr);
						my $response = $ua->request($req);
						if($response->is_success()) {
							my $page = $response->decoded_content();
							while($page =~ /$BITBUCKET_COMPILED_PATTERN/g) {
								$workspace = $1;
								$repoSlug = $2;
								last;
							}
						}
					}
				}
			}
		}
	};
	
	return ($isUri,$workspace,$repoSlug);
}


use constant BITBUCKET_ENDPOINT => 'https://'.BITBUCKET_HOST;
use constant BITBUCKET_API_ENDPOINT => 'https://api.'.BITBUCKET_HOST.'/2.0';
use constant BITBUCKET_API_ACCEPT_HEADER => '*/*;q=0.2,application/json';

use constant BITBUCKET_IO_HOST => 'bitbucket.io';

# https://developer.atlassian.com/bitbucket/api/2/reference/resource/users/%7Busername%7D
sub getBitBucketUser($;$) {
	my $self = shift;
	
	my($username,$endpoint) = @_;
	my $p_bitBucketUserCache = $self->{'bitbucketUserCache'};
	
	$endpoint = BITBUCKET_API_ENDPOINT  unless(defined($endpoint));
	
	if(exists($p_bitBucketUserCache->{$username})) {
		return (1,$p_bitBucketUserCache->{$username});
	} else {
		my $ua = $self->{'ua'};
		my $config = $self->{'config'};
		my $authUser = $config->val(BITBUCKET_KIND,'user-slug');
		my $authToken = $config->val(BITBUCKET_KIND,'user-token');
		
		my $userUri = URI->new($endpoint);
		$userUri->path_segments($userUri->path_segments(),"users",$username);
		
		my($userSuccess,$userData) = $self->fetchJSON($userUri,$authUser,$authToken,BITBUCKET_API_ACCEPT_HEADER);
		
		$p_bitBucketUserCache->{$username} = $userData  if($userSuccess);
		
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
	
	my($workspace,$repoSlug) = @{$fullrepo}{('owner','repo')};
	my $ua = $self->{'ua'};
	
	my $lcWorkspace = lc($workspace);
	my $lcRepoSlug = lc($repoSlug);
	
	my $p_bitbucketRepoDataCache = $self->{'bitbucketRepoDataCache'};
	
	$p_bitbucketRepoDataCache->{$lcWorkspace} = {}  unless(exists($p_bitbucketRepoDataCache->{$lcWorkspace}));
	
	unless(exists($p_bitbucketRepoDataCache->{$lcWorkspace}{$lcRepoSlug})) {
		my $config = $self->{'config'};
		my $bb_user = $config->val(BITBUCKET_KIND,'user-slug');
		my $bb_token = $config->val(BITBUCKET_KIND,'user-token');
		my $pageSize = $config->val(BITBUCKET_KIND,'page-size',100);
		
		my %ans = ();
		
		print STDERR " * Processing $workspace $repoSlug\n";
		
		# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D
		my $repoUri = URI->new(BITBUCKET_API_ENDPOINT);
		$repoUri->path_segments($repoUri->path_segments(),"repositories",$workspace,$repoSlug);
		my($repoSuccess,$repoData) = $self->fetchJSON($repoUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
		
		my $minProcessing = !$repoSuccess;
		
		if($repoSuccess) {
			print STDERR "\t- Extended processing $workspace $repoSlug\n";
			# The real repo and/or owner could be different from the registered one
			my $realOwner;
			my $lcRealOwner;
			my $realWorkspace;
			my $realRepoSlug;
			my $lcRealRepoSlug;
			my $followProcessing = 1;
			
			if(exists($repoData->{'owner'}) && ref($repoData->{'owner'}) eq 'HASH') {
				if(exists($repoData->{'owner'}{'nickname'})) {
					$realOwner = $repoData->{'owner'}{'nickname'};
					$lcRealOwner = lc($realOwner);
					
					$p_bitbucketRepoDataCache->{$lcRealOwner} = {}  unless(exists($p_bitbucketRepoDataCache->{$lcRealOwner}));
					
					if(exists($repoData->{'slug'})) {
						$realRepoSlug = $repoData->{'slug'};
						$lcRealRepoSlug = lc($realRepoSlug);
					}
				}
				
				if(exists($repoData->{'owner'}{'uuid'})) {
					$realWorkspace = $repoData->{'owner'}{'uuid'};
					$p_bitbucketRepoDataCache->{$realWorkspace} = $p_bitbucketRepoDataCache->{$lcRealOwner}
				}
			}
			
			if(defined($realRepoSlug)) {
				if($lcRealOwner ne $lcWorkspace || $lcRealRepoSlug ne $lcRepoSlug) {
					if(exists($p_bitbucketRepoDataCache->{$lcRealOwner}{$lcRealRepoSlug})) {
						# Nothing more to do, work previously done
						$p_bitbucketRepoDataCache->{$lcWorkspace}{$lcRepoSlug} = $p_bitbucketRepoDataCache->{$lcRealOwner}{$lcRealRepoSlug};
						
						$followProcessing = undef;
					} else {
						# Two for one bargain!
						$p_bitbucketRepoDataCache->{$lcRealOwner}{$lcRealRepoSlug} =  \%ans;
						$p_bitbucketRepoDataCache->{$lcWorkspace}{$lcRepoSlug} = \%ans;
					}
				} else {
					$p_bitbucketRepoDataCache->{$lcRealOwner}{$lcRealRepoSlug} =  \%ans;
				}
			} else {
				# Nothing more to do here, min processing
				$followProcessing = undef;
				$minProcessing = 1;
			}
			
			if($followProcessing && exists($repoData->{'links'}) && ref($repoData->{'links'}) eq 'HASH') {
				$ans{'owner'} = $realOwner;
				$ans{'workspace'} = $realWorkspace;
				$ans{'repo'} = $realRepoSlug;
				
				$ans{'creationDate'} = $repoData->{'created_on'};
				$ans{'lastUpdate'} = $repoData->{'updated_on'};
				
				$ans{'vcs_type'} = $repoData->{'scm'};
				# sourcecode repository
				
				if(exists($repoData->{'links'}{'clone'}) && ref($repoData->{'links'}{'clone'}) eq 'ARRAY') {
					foreach my $cloneP (@{$repoData->{'links'}{'clone'}}) {
						if(exists($cloneP->{'href'}) && exists($cloneP->{'name'}) && $cloneP->{'name'} eq 'https') {
							$ans{'vcs_uri'} = $cloneP->{'href'};
							# sourcecode
							$ans{'source_uri'} = $cloneP->{'href'};
							last;
						}
					}
				}
				# sourcecode download registration
				$ans{'source_isDownloadRegistered'} = JSON->false;
				# sourcecode anonymous access
				$ans{'source_hasAnonymousAccess'} = $repoData->{'is_private'} ? JSON->false : JSON->true;
				# sourcecode repository browse
				$ans{'source_isRepoBrowsable'} = JSON->true;
				# Source_code repository version controlled
				$ans{'source_hasVcsRepo'} = JSON->true;
				
				# TO IMPROVE
				# Source code repository accessible
				$ans{'source_isRepoAccessible'} = JSON->true;
				
				# website
				if(exists($repoData->{'website'}) && defined($repoData->{'website'}) && length($repoData->{'website'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'website'};
				} elsif(exists($repoData->{'links'}{'html'}) && defined($repoData->{'links'}{'html'}{'href'}) && length($repoData->{'links'}{'html'}{'href'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'links'}{'html'}{'href'};
				}
				
				if(exists($repoData->{'description'}) && defined($repoData->{'description'})) {
					# canonical:description
					$ans{'desc'} = $repoData->{'description'};
					# canonical:concept
					$ans{'concept'} = $repoData->{'description'};
				}
				
				# canonical|version:HELP
				$ans{'has_help'} = (exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'})? JSON->true : JSON->false;
				# canonical|version:Tutorial
				$ans{'has_tutorial'} = (exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'}) ? JSON->true : JSON->false;
				
				if($repoData->{'has_issues'}) {
					# issue tracker
					$ans{'has_issue_tracker'} = JSON->true;

					my $issuesUri = undef;
					
					if(exists($repoData->{'links'}{'issues'})) {
						$issuesUri = URI->new($repoData->{'links'}{'issues'}{'href'});
					} else {
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/issues
						$issuesUri = URI->new(BITBUCKET_API_ENDPOINT);
						$issuesUri->path_segments($issuesUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'issues');
					}
					$issuesUri->query_form(BITBUCKET_PAGELEN() => $pageSize);
					
					# Now, accounting
					my $numOpen = 0;
					my $numClosed = 0;
					my $lastClosed = undef;
					my $lastUpdated = undef;
					my $lastCreated = undef;
					
					my $minDuration = undef;
					my $maxDuration = undef;
					my $totalDuration = DateTime::Duration->new('seconds' => 0);
					
					my $isoparser = $self->{'isoparser'};
					my $total = undef;
					do {
						my($issuesSuccess,$issuesData) = $self->fetchJSON($issuesUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
						
						$issuesUri = undef;
						# It gathers the date of the last issue, and the date of the last update
						if($issuesSuccess) {
							# First, check next iteration link
							$issuesUri = $issuesData->{'next'}  if(exists($issuesData->{'next'}));
							
							# This is redundant, but we are giving a change to record in course issues
							$total = $issuesData->{'size'};
							
							foreach my $issue (@{$issuesData->{'values'}}) {
								if($issue->{'state'} eq 'closed' || $issue->{'state'} eq 'resolved') {
									$numClosed++;
									my $closedDt = $isoparser->parse_datetime($issue->{'updated_on'});
									my $createdDt = $isoparser->parse_datetime($issue->{'created_on'});
									my $duration = $closedDt - $createdDt;
									my $absDuration = $closedDt->subtract_datetime_absolute($createdDt);
									
									$totalDuration = $totalDuration + $absDuration;
									if(!defined($minDuration) || DateTime::Duration->compare($minDuration,$duration) > 0) {
										$minDuration = $duration;
									}
									if(!defined($maxDuration) || DateTime::Duration->compare($duration,$maxDuration) > 0) {
										$maxDuration = $duration;
									}
									
									# We have to simulate the lastClosed based on the last update of those which are resolved or closed
									$lastClosed = $issue->{'updated_on'}  if(defined($issue->{'updated_on'}) && (!defined($lastClosed) || $lastClosed lt $issue->{'updated_on'}));
								} else {
									$numOpen++;
								}
								
								$lastCreated = $issue->{'created_on'}  if(defined($issue->{'created_on'}) && (!defined($lastCreated) || $lastCreated lt $issue->{'created_on'}));
								$lastUpdated = $issue->{'updated_on'}  if(defined($issue->{'updated_on'}) && (!defined($lastUpdated) || $lastUpdated lt $issue->{'updated_on'}));
							}
						}
					} while(defined($issuesUri));
					
					my %issues = (
						'total' => $total,
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
				
				# All the gathered versions and tags go to this array
				my @versions = ();
				if(exists($repoData->{'links'}{'downloads'})) {
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/tags
					my $releasesUri = URI->new($repoData->{'links'}{'downloads'}{'href'});
					#$releasesUri = URI->new(BITBUCKET_API_ENDPOINT);
					#$releasesUri->path_segments($tagsUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'downloads');
					$releasesUri->query_form(BITBUCKET_PAGELEN() => $pageSize);
					
					do {
						my($releasesSuccess,$releasesData) = $self->fetchJSON($releasesUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
						$releasesUri = undef;
						
						if($releasesSuccess) {
							# Versions
							$releasesUri = $releasesData->{'next'}  if(exists($releasesData->{'next'}));
							$ans{'binaries'} = []  unless(exists($ans{'binaries'}));
							foreach my $release (@{$releasesData->{'values'}}) {
								push(@versions,$release->{'name'});
								
								my $binaryUri = $release->{'links'}{'self'}{'href'};
								
								my $p_b = {
									'binary_uri' => $binaryUri,
									'binary_isDistributable' => JSON->true,
									'binary_isDownloadRegistered' => JSON->false,
									'creationDate' => $release->{'created_on'},
									'numDownloads' => $release->{'downloads'}
								};
								# binary distribution
								unless(exists($ans{'binary_uri'})) {
									$ans{'binary_uri'} = $binaryUri;
									# binary distribution freeness
									$ans{'binary_isDistributable'} = JSON->true;
									# binaries download registration
									$ans{'binary_isDownloadRegistered'} = JSON->false;
								}
								push(@{$ans{'binaries'}},$p_b);
							}
						}
					} while(defined($releasesUri));
				}
				
				my $tagsUri = undef;
				if(exists($repoData->{'links'}{'tags'})) {
					$tagsUri = URI->new($repoData->{'links'}{'tags'}{'href'});
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/tags
					#$tagsUri = URI->new(BITBUCKET_API_ENDPOINT);
					#$tagsUri->path_segments($tagsUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'refs','tags');
					$tagsUri->query_form(BITBUCKET_PAGELEN() => $pageSize);
					do {
						my($tagsSuccess,$tagsData) = $self->fetchJSON($tagsUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
						$tagsUri = undef;
						
						if($tagsSuccess) {
							# First, check next iteration link
							$tagsUri = $tagsData->{'next'}  if(exists($tagsData->{'next'}));
							
							# Tags treated as versions
							foreach my $tag (@{$tagsData->{'values'}}) {
								push(@versions,$tag->{'name'});
							}
						}
					} while($tagsUri);
				}
				
				if(exists($repoData->{'language'})) {
					# This can be improved using external programs
					$ans{'languages'} = [ $repoData->{'language'} ];
					
					my $interpreted = JSON->false;
					my $compiled = JSON->false;
					foreach my $lang (@{$ans{'languages'}}) {
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
					my $lcLanguage = lc($repoData->{'language'});
					if(exists($recognizedBuildSystemsByLang{$lcLanguage})) {
						$ans{'tool_buildSystem'} = $recognizedBuildSystemsByLang{$lcLanguage};
					}
				}
				
				my @contributors = ();
				if(exists($repoData->{'links'}{'commits'})) {
					my $contrUri = URI->new($repoData->{'links'}{'commits'}{'href'});
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/commits
					#my $contrUri = URI->new(BITBUCKET_API_ENDPOINT);
					#$contrUri->path_segments($contrUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'commits');
					$contrUri->query_form(BITBUCKET_PAGELEN() => $pageSize);
					
					my %contribHash = ();
					
					do {
						my($contrSuccess,$contrData) = $self->fetchJSON($contrUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
						$contrUri = undef;
						
						if($contrSuccess) {
							$contrUri = $contrData->{'next'}  if(exists($contrData->{'next'}));
							
							# credits
							foreach my $commit (@{$contrData->{'values'}}) {
								my $p_contrib = $commit->{'author'};

								my %contribOpEB = ();
								
								my $username = undef;
								my $name = $p_contrib->{'raw'};
								my $email = undef;
								
								if($name =~ /^([^\<]+) \<([^\>]+)\>$/) {
									$name = $1;
									$email = $2;
								}
								
								if(exists($p_contrib->{'user'})) {
									$username = $p_contrib->{'user'}{'nickname'};
									$name = $p_contrib->{'user'}{'display_name'};
								}
								
								unless(exists($contribHash{$name})) {
									my $p_contribOpEB = {
										'name' => $name
									};
									$p_contribOpEB->{'email'} = $email  if(defined($email));
									$p_contribOpEB->{'username'} = $username  if(defined($username));
									
									push(@contributors,$p_contribOpEB);
									$contribHash{$name} = undef;
								}
							}
						}
					} while(defined($contrUri));
				}
				
				if(exists($repoData->{'links'}{'forks'})) {
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/forks
					my $forksUri = $repoData->{'links'}{'forks'}{'href'};
					my($forksSuccess,$forksData) = $self->fetchJSON($forksUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
					
					if($forksSuccess) {
						$ans{'numForks'} = $forksData->{'size'};
					}
				}
				
				if(exists($repoData->{'links'}{'watchers'})) {
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/watchers
					my $watchersUri = $repoData->{'links'}{'watchers'}{'href'};
					my($watchersSuccess,$watchersData) = $self->fetchJSON($watchersUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
					
					if($watchersSuccess) {
						$ans{'numWatchers'} = $watchersData->{'size'};
					}
				}
				
				if(exists($repoData->{'links'}{'branches'})) {
					# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/branches
					my $branchesUri = URI->new($repoData->{'links'}{'branches'}{'href'});
					$branchesUri->query_form(BITBUCKET_PAGELEN() => $pageSize);
					
					# We read about all the branches, and we have a look at its head
					my $lastPush = '';
					my @branches = ();
					do {
						my($branchesSuccess,$branchesData) = $self->fetchJSON($branchesUri,$bb_user,$bb_token,BITBUCKET_API_ACCEPT_HEADER);
						$branchesUri = undef;
						
						if($branchesSuccess) {
							$branchesUri = $branchesData->{'next'}  if(exists($branchesData->{'next'}));
							
							foreach my $branch (@{$branchesData->{'values'}}) {
								my $p_br = {
									'name' => $branch->{'name'}
								};
								push(@branches,$p_br);
								
								if(exists($branch->{'target'}) && exists($branch->{'target'}{'date'})) {
									$p_br->{'lastPush'} = $branch->{'target'}{'date'};
									$lastPush = $p_br->{'lastPush'}  if($p_br->{'lastPush'} gt $lastPush);
								}
							}
						}
					} while(defined($branchesUri));
					
					# At last
					$ans{'lastPush'} = $lastPush;
					$ans{'branches'} = \@branches;
				}
				
				
				# TBD with an external program (sloooooooooooooooow)
				## sourcecode freeness. The default license is the owner itself
				#$ans{'source_isFree'} = JSON->false;
				#$ans{'has_license'} = defined($repoData->{'license'}) ? JSON->true : JSON->false;
				#
				#if($ans{'has_license'}) {
				#	my $licUri = URI->new(GITHUB_API_ENDPOINT);
				#	$licUri->path_segments("","repos",$realOwner,$realRepoSlug,'license');
				#	my($licSuccess,$licData) = $self->fetchJSON($licUri,$bb_user,$bb_token);
				#	if($licSuccess) {
				#		# sourcecode freeness
				#		$ans{'source_isFree'} = JSON->true;
				#		if(exists($licData->{'_links'}{'html'})) {
				#			# project license
				#			$ans{'tool_license_uri'} = $licData->{'_links'}{'html'};
				#			# sourcecode license
				#			$ans{'source_license_uri'} = $licData->{'_links'}{'html'};
				#		}
				#		
				#		if(exists($licData->{'license'}{'spdx_id'}) && defined($licData->{'license'}{'spdx_id'})) {
				#			$ans{'source_license'} = $licData->{'license'}{'spdx_id'};
				#		} elsif(exists($licData->{'license'}{'key'}) && defined($licData->{'license'}{'key'})) {
				#			$ans{'source_license'} = $licData->{'license'}{'key'};
				#		} elsif(exists($licData->{'license'}{'name'}) && defined($licData->{'license'}{'name'})) {
				#			$ans{'source_license'} = $licData->{'license'}{'name'};
				#		}
				#		
				#		# TO IMPROVE
				#		if($licData->{'license'}{'featured'}) {
				#			# 'open source'
				#			$ans{'is_opensource'} = JSON->true;
				#			# 'OSI'
				#			$ans{'is_OSI'} = JSON->true;
				#		}
				#	}
				#}
				
				# TBD (using command line tools?)
				## Looking for a README and other elements
				#my $commitUri = URI->new(GITHUB_API_ENDPOINT);
				#$commitUri->path_segments("","repos",$realOwner,$realRepoSlug,'commits');
				#my($commitSuccess,$commitData) = $self->fetchJSON($commitUri,$bb_user,$bb_token,1);
				#if($commitSuccess && scalar(@{$commitData}) > 0) {
				#	my $treeUri = $commitData->[0]{'commit'}{'tree'}{'url'};
				#	
				#	my($treeSuccess,$treeData) = $self->fetchJSON($treeUri,$bb_user,$bb_token);
				#	if($treeSuccess) {
				#		foreach my $elem (@{$treeData->{'tree'}}) {
				#			if(index(lc($elem->{'path'}),'readme') == 0) {
				#				$ans{'readmeFile'} = $elem->{'path'};
				#				last;
				#			}
				#		}
				#	}
				#}
				
				# Versions
				$ans{'tool_versions'} = \@versions  if(scalar(@versions) > 0);
				# credits
				$ans{'tool_developers'} = \@contributors  if(scalar(@contributors) > 0);
			}
		}
		
		# These are failed, strange cases
		if($minProcessing) {
			$p_bitbucketRepoDataCache->{$lcWorkspace}{$lcRepoSlug} = \%ans;
			
			# Alternate way
			my $clone_uri = URI->new(BITBUCKET_ENDPOINT);
			$clone_uri->path_segments($clone_uri->path_segments(),$workspace,$repoSlug);
			
			# TODO: slow check whether it is git or mercurial through command line
			$ans{'vcs_type'} = 'git';
			# sourcecode repository
			$ans{'vcs_uri'} = $clone_uri->as_string();
			
			$ans{'owner'} = $workspace;
			$ans{'workspace'} = $workspace;
			$ans{'repo'} = $repoSlug;
		}
	}
	
	return $p_bitbucketRepoDataCache->{$lcWorkspace}{$lcRepoSlug};
}	

1;
