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

	# Only when the file exists
	if [ -f "$toolsFile" ] ; then
		# But not the working directory
		if [ ! -d "$workdDir" ] ; then
			mkdir -p "$workDir"
			perl "${SCRIPTDIR}"/repoEnricher.pl -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFile"
			echo "${SHELL}" "${SCRIPTDIR}"/opeb-submitter/repo_result_submitter.bash "${SCRIPTDIR}"/opeb-submitter/cron-submitter.ini "$workDir"
		fi
	fi
else
	echo "ERROR: This script needs two parameters: a workdir and the input tools file" 1>&2
fi
