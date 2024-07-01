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

if [ $# -eq 3 ] ; then
	parentWorkDir="$1"
	toolsFile="$2"
	relWorkDir="$3"
	workDir="$1/$3"
	toolsFileXZ="${toolsFile}.xz"

	exec >> "${parentWorkDir}/startlog.txt" 2>&1
	echo "[$(date -Is)] Launching repoEnricher"
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
	retval=0
	if [ ! -d "$workDir" ] ; then
		mkdir -p "$workDir"
		exec >> "${workDir}/log.txt" 2>&1
		set +e
		# perl "${SCRIPTDIR}"/repoEnricher.pl -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ"
		python3 "${SCRIPTDIR}"/repoEnricher.py -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ"
		retval=$?
		set -e
	fi
	if [ "$retval" = 0 ] ; then
		/bin/bash "${SCRIPTDIR}"/opeb-submitter/repo_result_submitter.bash "${cronSubmitterConfig}" "$workDir"
		
		# This is needed to keep a copy of the source along with the results
		xz -c "$toolsFileXZ" > "${workDir}"/"$(basename "${toolsFile}")"
		tar -C "${parentWorkDir}" -c -p -f - "${relWorkDir}" | xz -9 -c -T0 > "${parentWorkDir}"/"$(basename "$(dirname "$0")")"-"${relWorkDir}".tar.xz
		rm -rf "${workDir}"
	else
		echo "INFO: Data submission has been suspended, as the enriching process did not finish properly" 1>&2
	fi
else
	echo "ERROR: This script needs three parameters: a parent workdir, the input tools file and a relative subdirectory within the parent workdir" 1>&2
fi
