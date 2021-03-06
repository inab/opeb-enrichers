#!/bin/bash

exec > /tmp/prueba.txt 2>&1

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

if [ $# -eq 2 ] ; then
	workDir="$1"
	toolsFile="$2"
	toolsFileXZ="${toolsFile}.xz"

	if [ ! -f "$toolsFileXZ" ] ; then
		if [ ! -f "$toolsFile" ] ; then
			mkdir -p "$(dirname "$toolsFile")"
			wget -nv -O "$toolsFile" https://openebench.bsc.es/monitor/rest/search
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
		perl "${SCRIPTDIR}"/repoEnricher.pl -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ"
	fi
	/bin/bash "${SCRIPTDIR}"/opeb-submitter/repo_result_submitter.bash "${SCRIPTDIR}"/opeb-submitter/cron-submitter.ini "$workDir"
else
	echo "ERROR: This script needs two parameters: a workdir and the input tools file" 1>&2
fi
