map(
	. as {
		"@id": $id,
		"repos": $repos
	}
	|
	$repos[0].res as $repo
	|
	{
		"@id": $id | split("/") | ( .[4] |= "metrics" ) | ( .[2] |= $host ) | join("/"),
		"project.readme": ( $repo | has("readmeFile") ),
		"distribution.binaries.freeness": $repo.binary_isDistributable,
		"distribution.binaries.registration": $repo.binary_isDownloadRegistered,
		"distribution.sourcecode.interpreted": $repo.has_interpretedLanguages,
		"distribution.sourcecode.license": $repo.has_license,
		"distribution.sourcecode.readme": ( $repo | has("readmeFile") ),
		"distribution.sourcecode.free": $repo.source_hasVcsRepo,
		"distribution.sourcecode.public": ($repo.source_uri? != ""),
		"distribution.sourcecode.repository.online": $repo.source_isRepoBrowsable,
		"distribution.sourcecode.repository.anonymous": $repo.source_hasAnonymousAccess,
		"distribution.packages": $repo.binary_isDistributable,
		"distribution.source_packages": (( $repo.tool_versions | length ) > 0 ),
		"support.issue_tracking.public_tracker": $repo.has_issue_tracker,
		"support.issue_tracking.resolve_time": $repo.issues.meanDuration,
	}
)
