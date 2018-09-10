#!/usr/bin/perl

use v5.10.1;
use strict;
use warnings 'all';

use FindBin;
use lib File::Spec->catfile($FindBin::Bin,'libs');

use OpenEBenchQueries;

use Carp;
use Config::IniFiles;

use Time::HiRes;

use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;
use Scalar::Util qw(blessed);
use Getopt::Long;

use File::Path qw();
use File::Spec;

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

use constant GITHUB_HOST => 'github.com';
use constant GITHUB_ENDPOINT => 'https://'.GITHUB_HOST;
use constant GITHUB_API_ENDPOINT => 'https://api.github.com/';

use constant GITHUB_IO_HOST => 'github.io';

use constant GITHUB_API_V_HEADER => 'application/vnd.github.v3+json';
# KeyOrder

my @TabKeyOrder = (
	['tool_id','Tool id'],
	['vcs_type','Repository Type'],
	['vcs_uri','sourcecode repository'],
	['source_uri','sourcecode'],
	['source_isFree','sourcecode freeness'],
	['source_isDownloadRegistered','sourcecode download registration'],
	['source_hasAnonymousAccess','sourcecode anonymous access'],
	['source_isRepoBrowsable','sourcecode repository browse'],
	['source_hasVcsRepo','Source_code repository version controlled'],
	['source_isRepoAccessible','Source code repository accessible'],
	
	['has_website','website'],
	['website','website url'],
	['desc','canonical:description'],
	['concept','canonical:concept'],
	['has_help','canonical|version:HELP'],
	['has_tutorial','canonical|version:Tutorial'],
	['has_issue_tracker','issue tracker'],
	
	['binary_uri','binary distribution'],
	['binary_isDistributable','binary distribution freeness'],
	['binary_isDownloadRegistered','binaries download registration'],
	['tool_versions','Versions'],
	
	['tool_buildSystem','type'],
	
	['tool_license_uri','project license'],
	['source_license_uri','sourcecode license'],
	['source_license','sourcecode license name'],
	['is_opensource','open source'],
	['is_OSI','OSI'],
	
	# This must be the last
	['tool_developers','credits']
);

sub fetchJSON($;$$$$$) {
	my($bUri,$ua,$user,$token,$p_acceptHeaders,$numIter) = @_;
	
	$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
	$numIter = 0  unless(defined($numIter));
	
	my $uriStr = (blessed($bUri) && $bUri->can('as_string')) ? $bUri->as_string() : $bUri;
	my $response;
	my $bData = undef;
	my $j = JSON::MaybeXS->new('convert_blessed' => 1);
	
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
		# Time::HiRes::usleep(720);
		sleep(1);
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
			print STDERR "ERROR: kicked out $bUriStr : ".$response->status_line()."\n";
		}
	} while(defined($uriStr) && $numIter != 0);
	
	return ($response->is_success(),$bData);
}

{
	my %gitHubUserCache = ();

	sub getGitHubUser($;$$$) {
		my($username,$ua,$user,$token) = @_;
		
		if(exists($gitHubUserCache{$username})) {
			return (1,$gitHubUserCache{$username});
		} else {
			$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
			
			my $userUri = URI->new(GITHUB_API_ENDPOINT);
			$userUri->path_segments("","users",$username);
			
			my($userSuccess,$userData) = fetchJSON($userUri,$ua,$user,$token,GITHUB_API_V_HEADER);
			
			$gitHubUserCache{$username} = $userData  if($userSuccess);
			
			return ($userSuccess,$userData);
		}
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

{
	# It is case insensitive
	my %githubRepoDataCache = ();
	
	my $isoparser = DateTime::Format::ISO8601->new();
	
	sub getGitHubRepoData(\%$$;$) {
		my($fullrepo,$gh_user,$gh_token,$ua) = @_;
		
		my($owner,$repo) = @{$fullrepo}{('owner','repo')};
		$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
		
		my $lcOwner = lc($owner);
		my $lcRepo = lc($repo);
		
		$githubRepoDataCache{$lcOwner} = {}  unless(exists($githubRepoDataCache{$lcOwner}));
		
		unless(exists($githubRepoDataCache{$lcOwner}{$lcRepo})) {
			my %ans = ();
			
			# These are all the URIs the program needs to fetch from GitHub
			
			print STDERR " * Processing $owner $repo\n";
			my $repoUri = URI->new(GITHUB_API_ENDPOINT);
			$repoUri->path_segments("","repos",$owner,$repo);
			my($repoSuccess,$repoData) = fetchJSON($repoUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			
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
					
					$githubRepoDataCache{$lcRealOwner} = {}  unless(exists($githubRepoDataCache{$lcRealOwner}));
					
					if(exists($repoData->{'name'})) {
						$realRepo = $repoData->{'name'};
						$lcRealRepo = lc($realRepo);
					}
				}
				
				if(defined($realRepo)) {
					if($lcRealOwner ne $lcOwner || $lcRealRepo ne $lcRepo) {
						if(exists($githubRepoDataCache{$lcRealOwner}{$lcRealRepo})) {
							# Nothing more to do, work previously done
							$githubRepoDataCache{$lcOwner}{$lcRepo} = $githubRepoDataCache{$lcRealOwner}{$lcRealRepo};
							
							$followProcessing = undef;
						} else {
							# Two for one bargain!
							$githubRepoDataCache{$lcRealOwner}{$lcRealRepo} =  \%ans;
							$githubRepoDataCache{$lcOwner}{$lcRepo} = \%ans;
						}
					} else {
						$githubRepoDataCache{$lcRealOwner}{$lcRealRepo} =  \%ans;
					}
				} else {
					# Nothing more to do here, min processing
					$followProcessing = undef;
					$minProcessing = 1;
				}
				
				if($followProcessing) {
					if(exists($repoData->{'clone_url'}) && defined($repoData->{'clone_url'})) {
						$ans{'owner'} = $realOwner;
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
						my($issuesSuccess,$issuesData) = fetchJSON($issuesUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
						
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
					my($releasesSuccess,$releasesData) = fetchJSON($releasesUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					if($releasesSuccess) {
						# Versions
						foreach my $release (@{$releasesData}) {
							push(@versions,$release->{'tag_name'});
							
							if(exists($release->{'assets'}) && !exists($ans{'binary distribution'})) {
								foreach my $asset (@{$release->{'assets'}}) {
									# binary distribution
									$ans{'binary_uri'} = $asset->{'browser_download_url'};
									# binary distribution freeness
									$ans{'binary_isDistributable'} = JSON->true;
									# binaries download registration
									$ans{'binary_isDownloadRegistered'} = JSON->false;
									last;
								}
							}
						}
					}
					
					
					my $tagsUri = URI->new(GITHUB_API_ENDPOINT);
					$tagsUri->path_segments("","repos",$realOwner,$realRepo,'tags');
					my($tagsSuccess,$tagsData) = fetchJSON($tagsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					if($tagsSuccess) {
						# Tags treated as versions
						my %vercheck = map { $_ => undef } @versions;
						foreach my $tag (@{$tagsData}) {
							push(@versions,$tag->{'name'})  unless(exists($vercheck{$tag->{'name'}}));
						}
					}
					
					my $langsUri = URI->new(GITHUB_API_ENDPOINT);
					$langsUri->path_segments("","repos",$realOwner,$realRepo,'languages');
					my($langsSuccess,$langsData) = fetchJSON($langsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
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
					my($contrSuccess,$contrData) = fetchJSON($contrUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					if($contrSuccess) {
						# credits
						foreach my $contributor (@{$contrData}) {
							my $p_contrib = getGitHubUser($contributor->{'login'},$ua,$gh_user,$gh_token);

							my %contribOpEB = (
								'username'	=> $contributor->{'login'}
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
						my($licSuccess,$licData) = fetchJSON($licUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
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
					my($commitSuccess,$commitData) = fetchJSON($commitUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER,1);
					if($commitSuccess && scalar(@{$commitData}) > 0) {
						my $treeUri = $commitData->[0]{'commit'}{'tree'}{'url'};
						
						my($treeSuccess,$treeData) = fetchJSON($treeUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
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
					#my($pullsSuccess,$pullsData) = fetchJSON($pullsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
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
				$githubRepoDataCache{$lcOwner}{$lcRepo} = \%ans;
				
				# Alternate way
				my $clone_uri = URI->new(GITHUB_ENDPOINT);
				$clone_uri->path_segments('',$owner,$repo);
				$ans{'vcs_type'} = 'git';
				# sourcecode repository
				$ans{'vcs_uri'} = $clone_uri->as_string();
				
				$ans{'owner'} = $owner;
				$ans{'repo'} = $repo;
			}
		}
		
		return $githubRepoDataCache{$lcOwner}{$lcRepo};
	}	
}


my @opebTools = ();

my $save_opeb_filename = undef;
my $load_opeb_filename = undef;
my $config_filename = undef;

my $tabfile = undef;
my $jsondir = undef;
Getopt::Long::GetOptions(
	"save-opeb:s"	=>	\$save_opeb_filename,
	"use-opeb:s"	=>	\$load_opeb_filename,
	"config|C:s"	=>	\$config_filename,
	"file|f:s"	=>	\$tabfile,
	"directory|D:s"	=>	\$jsondir,
) or die("Error in command line arguments\n");

my $config;
my $gh_user = undef;
my $gh_token = undef;
if(defined($config_filename)) {
	$config = Config::IniFiles->new( -file => $config_filename );
	if(defined($config)) {
		$gh_user = $config->val('github','gh-user');
		$gh_token = $config->val('github','gh-token');
	} else {
		foreach my $croak (@Config::IniFiles::errors) {
			Carp::carp($croak);
		}
		exit 1;
	}
} else {
	# Empty config
	$config = Config::IniFiles->new();
}

if(defined($jsondir) || defined($tabfile)) {
	my $opEb = OpenEBenchQueries->new($load_opeb_filename,$save_opeb_filename);
	my $p_queries = $opEb->extractGitHubIds();
	
	if(scalar(@{$p_queries})>0) {
		my $jsonManifestFile = undef;
		my @manifest = ();
		if(defined($jsondir)) {
			File::Path::make_path($jsondir);
			$jsonManifestFile = File::Spec->catfile($jsondir,'manifest.json');
			print "* JSON output directory set to $jsondir . Manifest file is $jsonManifestFile\n";
		}
		
		my $TAB;
		my $jTAB;
		if(defined($tabfile)) {
			print "* Tabular output file set to $tabfile\n";
			open($TAB,'>:encoding(UTF-8)',$tabfile) or die("ERROR: Unable to create $tabfile: $!\n");
			$jTAB = JSON::MaybeXS->new();
		}
		
		unless(defined($jsondir) || defined($tabfile)) {
			print "* Default to tabular output file set to STDOUT\n";
			open($TAB,'>&:encoding(UTF-8)',\*STDOUT) or die("ERROR: Unable to redirect to STDOUT: $!\n");
		}
		
		print "* Processing ".scalar(@{$p_queries})." tools\n";
		
		my $printedHeader = undef;
		my $numTool = 0;
		my $ua = LWP::UserAgent->new();
		foreach my $query (@{$p_queries}) {
			# What we know, just now
			my %fullans = %{$query};
			
			foreach my $fullrepo (@{$fullans{'repos'}}) {
				my $p_ans = $fullrepo->{'res'} = getGitHubRepoData(%{$fullrepo},$gh_user,$gh_token,$ua);
				
				if(defined($TAB)) {
					unless($printedHeader) {
						$printedHeader = 1;
						print $TAB join("\t",map { $_->[1] } @TabKeyOrder),"\n";
						print $TAB join("\t",map { $_->[0] } @TabKeyOrder),"\n";
					}
					
					print $TAB join("\t",map {
						my $key = $_->[0];
						my $retval = '';
						if(exists($p_ans->{$key}) && defined($p_ans->{$key})) {
							$retval = $p_ans->{$key};
							if(ref($retval) eq 'ARRAY') {
								if(scalar(@{$retval}) > 0) {
									if(ref($retval->[0])) {
										$retval = join(',',map { $jTAB->encode($_) } @{$retval});
									} else {
										$retval = join(',',@{$retval});
									}
								}
							} elsif(ref($retval)) {
								$retval = $jTAB->encode($retval);
							}
						}
						$retval;
					} @TabKeyOrder),"\n";
				}
			}
			
			# The assembled answer
			if(defined($jsondir)) {
				my $partialJsonout = 'tool-'.$numTool.'.json';
				push(@manifest,{
					'@id'	=>	$fullans{'@id'},
					'file'	=>	$partialJsonout
				});
				my $jsonout = File::Spec->catfile($jsondir,$partialJsonout);
				if(open(my $J,'>:encoding(UTF-8)',$jsonout)) {
					print $J JSON::MaybeXS->new(pretty => 1,convert_blessed => 1)->encode(\%fullans);
					
					close($J);
				} else {
					Carp::croak("* ERROR: Unable to create file $jsonout. Reason: $!");
				}
			}
			# print JSON->new()->pretty(1)->encode(\%ans),"\n";
			
			# Another more
			$numTool++;
		}
		
		# Closing the output file (if any)
		close($TAB)  if(defined($TAB));
		
		# Writing the manifest
		if(defined($jsonManifestFile)) {
			if(open(my $M,'>:encoding(UTF-8)',$jsonManifestFile)) {
				print $M JSON::MaybeXS->new(pretty => 1)->encode(\@manifest);
				close($M);
			} else {
				Carp::croak("ERROR: Unable to write manifest $jsonManifestFile. Reason: $!");
			}
		}
	} else {
		print STDERR "No queries extracted from OpenEBench. Do you have internet access?\n";
	}
} else {
	print STDERR "Usage: $0 [-C config file] [-D destination directory | -f destination file] [--save-opeb save_opeb_file.json] [--use-opeb use_opeb_file.json]\n";
	exit 1;
}
