#!/bin/bash

set -e
SCRIPTDIR="$(dirname "$0")"

case "${SCRIPTDIR}" in
	/*)
		true
		;;
	*)
		SCRIPTDIR="${PWD}/${SCRIPTDIR}"
		;;
esac

# This is needed to select the right hosts
cronSubmitterConfig="${SCRIPTDIR}"/opeb-submitter/cron-submitter.ini
source "${cronSubmitterConfig}"
OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"

if [ $# -eq 2 ] ; then
	workDir="$1"
	toolsFile="$2"
	toolsFileXZ="${toolsFile}.xz"

	exec >> "$(dirname "${workDir}")/startlog.txt" 2>&1
	echo "[$(date -Is)] Launching pubEnricher"
	if [ ! -f "$toolsFileXZ" ] ; then
		if [ ! -f "$toolsFile" ] ; then
			mkdir -p "$(dirname "$toolsFile")"
			wget -nv -O "$toolsFile" https://${OPEB_HOST}/monitor/rest/search
		fi
		xz -9 -T 0 "${toolsFile}"
	fi

	# Only erase when the manifest file does not exist
	if [ ! -f "${workDir}/manifest.json" ] ; then
		rm -rf "${workDir}"
	fi
	# But not the working directory
	if [ ! -d "$workDir" ] ; then
		mkdir -p "$workDir"
		exec >> "${workDir}/log.txt" 2>&1
		perl "${SCRIPTDIR}"/repoEnricher.pl -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ"
	fi
	/bin/bash "${SCRIPTDIR}"/opeb-submitter/repo_result_submitter.bash "${cronSubmitterConfig}" "$workDir"
else
	echo "ERROR: This script needs two parameters: a workdir and the input tools file" 1>&2
fi
