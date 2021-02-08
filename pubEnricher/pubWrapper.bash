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

if [ $# -eq 2 ] ; then
	workDir="$1"
	parentCacheDir="$(dirname "$workDir")"
	toolsFile="$2"
	toolsFileXZ="${toolsFile}.xz"
	# This is needed to gather the logs
	exec >> "${parentCacheDir}/startlog.txt" 2>&1
	echo "[$(date -Is)] Launching pubEnricher"

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
	retval=0
	if [ ! -d "$workDir" ] ; then
		mkdir -p "$workDir"
		exec > "${workDir}/log.txt" 2>&1
		source "${SCRIPTDIR}"/.py3env/bin/activate
		set +e
		python "${SCRIPTDIR}"/pubEnricher.py -d -b meta -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ" "${parentCacheDir}"/pubCacheDir
		retval=$?
		set -e
	fi
	if [ "$retval" = 0 ] ; then
		"${SCRIPTDIR}"/opeb-submitter/result_submitter.bash "${SCRIPTDIR}"/opeb-submitter/cron-submitter.ini "$workDir"
	else
		echo "INFO: Data submission has been suspended, as the enriching process did not finish properly" 1>&2
	fi
	
else
	echo "ERROR: This script needs two parameters: a workdir and the destination path to the input tools file" 1>&2
fi
