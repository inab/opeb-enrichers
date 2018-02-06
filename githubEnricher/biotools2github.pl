#!/usr/bin/perl

use strict;
use warnings 'all';
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;
use Scalar::Util qw(blessed);
use Getopt::Long;
use File::Path qw();
use File::Spec;

use constant BIOTOOLS_ENDPOINT	=> URI->new('https://bio.tools/api/');
use constant GITHUB_HOST => 'github.com';
use constant GITHUB_ENDPOINT => 'https://'.GITHUB_HOST;
use constant GITHUB_API_ENDPOINT => 'https://api.github.com/';

use constant GITHUB_IO_HOST => 'github.io';


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

my $GITHUB_PATTERN = ':\/\/'.GITHUB_HOST.'\/([^"\'\/]+)\/([^"\'\/]+)';

my $GITHUB_COMPILED_PATTERN = qr/$GITHUB_PATTERN/;

sub matchGitHub($;$) {
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
				if($ghURI->host() eq GITHUB_HOST) {
					my @path = $ghURI->path_segments();
					if(scalar(@path)>=3) {
						($owner,$repo) = @path[1,2];
						
						# If it ends in \.git
						$repo = substr($repo,0,-4)  if($repo =~ /.git$/);
					}
				} elsif(substr($ghURI->host(),-length(GITHUB_IO_HOST)) eq GITHUB_IO_HOST) {
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
	};
	
	return ($isUri,$owner,$repo);
}

sub fetchJSON($;$$$) {
	my($bUri,$ua,$user,$token) = @_;
	
	$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
	my $bUriStr = (blessed($bUri) && $bUri->can('as_string')) ? $bUri->as_string() : $bUri;
	my $req = HTTP::Request->new('GET' => $bUriStr);

print STDERR "JARL $bUriStr $user $token\n";
	$req->authorization_basic($user,$token)  if(defined($user));
	
	sleep(1);
	my $response = $ua->request($req);
	
	my $bData = undef;
	if($response->is_success()) {
		eval {
			$bData = JSON->new()->utf8(1)->decode($response->decoded_content);
		};
		
		if($@) {
			print STDERR "ERROR: JSON parsing error: $@\n";
		}
	} else {
		print STDERR "ERROR: kicked out:".$response->status_line()."\n";
	}
	
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
			
			my($userSuccess,$userData) = fetchJSON($userUri,$ua,$user,$token);
			
			$gitHubUserCache{$username} = $userData  if($userSuccess);
			
			return ($userSuccess,$userData);
		}
	}
}

my @btIds = ();

my @filenames = ();
my $tabfile = undef;
my $jsondir = undef;
my $gh_user = undef;
my $gh_token = undef;
Getopt::Long::GetOptions(
	"gh-user=s"	=>	\$gh_user,
	"gh-token=s"	=>	\$gh_token,
	"tabular=s"	=>	\$tabfile,
	"jsondir=s"	=>	\$jsondir,
	"file=s"	=>	\@filenames,
) or die("Error in command line arguments\n");

if(scalar(@filenames) > 0 || scalar(@ARGV) > 0) {
	if(scalar(@filenames) > 0) {
		foreach my $file (@filenames) {
			if(open(my $IF,'<:encoding(UTF-8)',$file)) {
				print "* Reading identifiers file $file\n";
				
				while(my $line=<$IF>) {
					next if(substr($line,0,1) eq '#');
					chomp($line);
					
					push(@btIds,$line);
				}
				
				close($IF);
			} else {
				print STDERR "ERROR: Unable to open identifiers file $file. Reason: $!\n";
			}
		}
	}
	
	push(@btIds, @ARGV)  if(scalar(@ARGV)>0);
}

print STDERR "JARL $gh_user $gh_token\n";


if(scalar(@btIds)>0) {
	# Trimming spaces on the potential identifier, and splitting multiple ones
	foreach my $biotoolsId (@btIds) {
		$biotoolsId =~ s/^\s+//;
		$biotoolsId =~ s/\s+$//;
		
		# Splitting by ;
		my @possibleIds = split(/;/,$biotoolsId);
		# And keeping the first one
		if(scalar(@possibleIds)>1) {
			$biotoolsId = shift(@possibleIds);
			push(@btIds,@possibleIds)  if(scalar(@possibleIds)>0);
		}
	}
	
	my $jsonManifestFile = undef;
	my %manifest = ();
	if(defined($jsondir)) {
		File::Path::make_path($jsondir);
		$jsonManifestFile = File::Spec->catfile($jsondir,'manifest.json');
		print "* JSON output directory set to $jsondir . Manifest file is $jsonManifestFile\n";
	}
	
	my $TAB;
	if(defined($tabfile)) {
		print "* Tabular output file set to $tabfile\n";
		open($TAB,'>:encoding(UTF-8)',$tabfile) or die("ERROR: Unable to create $tabfile: $!\n");
	}
	
	unless(defined($jsondir) || defined($tabfile)) {
		print "* Default to tabular output file set to STDOUT\n";
		open($TAB,'>&:encoding(UTF-8)',\*STDOUT) or die("ERROR: Unable to redirect to STDOUT: $!\n");
	}
	
	print "* Processing ".scalar(@btIds)." identifiers\n";
	my $ua = LWP::UserAgent->new();
	
	my $printedHeader = undef;
	my $numTool = 0;
	foreach my $biotoolsId (@btIds) {
		my @githubURIs = ();
		
		# Is an URI from GitHub?
		{
			my($isUri,$owner,$repo) = matchGitHub($biotoolsId,$ua);
			
			if($isUri) {
				if(defined($owner) && defined($repo)) {
					push(@githubURIs, [$owner,$repo,$biotoolsId]);
				} else {
					print STDERR "Unrecognized URI $biotoolsId . Skipping\n";
					next;
				}
			}
		};
		
		# Let's suppose it is a biotoolsId
		if(scalar(@githubURIs)==0) {
			my $bUri = URI->new_abs($biotoolsId,BIOTOOLS_ENDPOINT);
			$bUri->query_form('format'=>'json');
			
			my($netSuccess,$bData) = fetchJSON($bUri,$ua);
			if($netSuccess) {
				if(defined($bData)) {
					if(exists($bData->{'link'}) && ref($bData->{'link'}) eq 'ARRAY') {
						# Iterate over all the possible links
						foreach my $link (@{$bData->{'link'}}) {
							my($isUri,$owner,$repo) = matchGitHub($link->{'url'},$ua);
							
							if($isUri && defined($owner) && defined($repo)) {
								push(@githubURIs, [$owner,$repo,$biotoolsId]);
							}
						}
					}
				} else {
					print STDERR "ERROR: JSON parsing error\n";
				}
			} else {
				print STDERR "ERROR: Unable to get record $biotoolsId from bio.tools. Does it exist?\n";
			}
		}
		
		# Now, let's work with the GitHub API
		my $p_mani = $manifest{$biotoolsId} = [];
		foreach my $repoData (@githubURIs) {
			my($owner,$repo,$biotoolsId) = @{$repoData};
			
			# What we now, just now
			my %ans = (
				'tool_id'	=>	$biotoolsId
			);
			
			# These are all the URIs the program needs to fetch from GitHub
			
			my $repoUri = URI->new(GITHUB_API_ENDPOINT);
			$repoUri->path_segments("","repos",$owner,$repo);
			my($repoSuccess,$repoData) = fetchJSON($repoUri,$ua,$gh_user,$gh_token);
			if($repoSuccess) {
				if(exists($repoData->{'clone_url'}) && defined($repoData->{'clone_url'})) {
					$ans{'vcs_type'} = 'git';
					# sourcecode repository
					$ans{'vcs_uri'} = $repoData->{'clone_url'};
					# sourcecode
					$ans{'source_uri'} = $repoData->{'clone_url'};
					# sourcecode freeness
					$ans{'source_isFree'} = JSON->true;
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
				}
			} else {
				# Alternate way
				my $clone_uri = URI->new(GITHUB_ENDPOINT);
				$clone_uri->path_segments('',$owner,$repo);
				$ans{'vcs_type'} = 'git';
				# sourcecode repository
				$ans{'vcs_uri'} = $clone_uri->as_string();
			}
			
			# All the gathered version tags go to this array
			my @versions = ();
			
			my $releasesUri = URI->new(GITHUB_API_ENDPOINT);
			$releasesUri->path_segments("","repos",$owner,$repo,'releases');
			my($releasesSuccess,$releasesData) = fetchJSON($releasesUri,$ua,$gh_user,$gh_token);
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
			$tagsUri->path_segments("","repos",$owner,$repo,'tags');
			my($tagsSuccess,$tagsData) = fetchJSON($tagsUri,$ua,$gh_user,$gh_token);
			if($tagsSuccess) {
				# Versions
				foreach my $tag (@{$tagsData}) {
					push(@versions,$tag->{'name'});
				}
			}
			
			my %recognizedBuildSystemsByLang = (
				'Makefile'	=>	'make'
			);
			
			my $langsUri = URI->new(GITHUB_API_ENDPOINT);
			$langsUri->path_segments("","repos",$owner,$repo,'languages');
			my($langsSuccess,$langsData) = fetchJSON($langsUri,$ua,$gh_user,$gh_token);
			if($langsSuccess) {
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
			$contrUri->path_segments("","repos",$owner,$repo,'contributors');
			my($contrSuccess,$contrData) = fetchJSON($contrUri,$ua,$gh_user,$gh_token);
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
			
			
			my $licUri = URI->new(GITHUB_API_ENDPOINT);
			$licUri->path_segments("","repos",$owner,$repo,'license');
			my($licSuccess,$licData) = fetchJSON($licUri,$ua,$gh_user,$gh_token);
			if($licSuccess) {
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
			
			
			#my $pullsUri = URI->new(GITHUB_API_ENDPOINT);
			#$pullsUri->path_segments("","repos",$owner,$repo,'pulls');
			#my($pullsSuccess,$pullsData) = fetchJSON($pullsUri,$ua,$gh_user,$gh_token);
			#if($pullsSuccess) {
			#	use Data::Dumper;
			#	print Dumper($pullsData),"\n";
			#}
			#
			#
			#my $issueUri = URI->new(GITHUB_API_ENDPOINT);
			#$issueUri->path_segments("","repos",$owner,$repo,'issues');
			#my($issueSuccess,$issueData) = fetchJSON($issueUri,$ua,$gh_user,$gh_token);
			#if($issueSuccess) {
			#	use Data::Dumper;
			#	print Dumper($issueData),"\n";
			#}
			
			# Versions
			$ans{'tool_versions'} = \@versions  if(scalar(@versions) > 0);
			# credits
			$ans{'tool_developers'} = \@contributors  if(scalar(@contributors) > 0);
			
			# The assembled answer
			if(defined($jsondir)) {
				my $partialJsonout = 'tool-'.$numTool.'.json';
				push(@{$p_mani},$partialJsonout);
				my $jsonout = File::Spec->catfile($jsondir,$partialJsonout);
				if(open(my $J,'>:encoding(UTF-8)',$jsonout)) {
					print $J JSON->new()->pretty(1)->encode(\%ans);
					
					close($J);
				} else {
					print STDERR "* ERROR: Unable to create file $jsonout. Reason: $!\n";
				}
			}
			# print JSON->new()->pretty(1)->encode(\%ans),"\n";
			
			if(defined($TAB)) {
				my $j = JSON->new();
				unless($printedHeader) {
					$printedHeader = 1;
					print $TAB join("\t",map { $_->[1] } @TabKeyOrder),"\n";
					print $TAB join("\t",map { $_->[0] } @TabKeyOrder),"\n";
				}
				
				print $TAB join("\t",map {
					my $key = $_->[0];
					my $retval = '';
					if(exists($ans{$key}) && defined($ans{$key})) {
						$retval = $ans{$key};
						if(ref($retval) eq 'ARRAY') {
							if(scalar(@{$retval}) > 0) {
								if(ref($retval->[0])) {
									$retval = join(',',map { $j->encode($_) } @{$retval});
								} else {
									$retval = join(',',@{$retval});
								}
							}
						} elsif(ref($retval)) {
							$retval = $j->encode($retval);
						}
					}
					$retval;
				} @TabKeyOrder),"\n";
			}
			
			# Another more
			$numTool++;
		}
	}
	
	# Closing the output file (if any)
	close($TAB)  if(defined($TAB));
	
	# Writing the manifest
	if(defined($jsonManifestFile)) {
		if(open(my $M,'>:encoding(UTF-8)',$jsonManifestFile)) {
			print $M JSON->new()->pretty(1)->encode(\%manifest);
			close($M);
		} else {
			print STDERR "ERROR: Unable to write manifest $jsonManifestFile. Reason: $!\n";
		}
	}
} else {
	print STDERR "Usage: $0 {bio.tools.id}+\n";
	exit 1;
}
