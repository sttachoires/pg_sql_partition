#!/bin/bash

genres=$1
allofthem="$(ls -1 partition_*.sql check_*.sql | cut -d'_' -f2 | cut -d'.' -f1 |sort -u)"
if [ "${genres}" = "" ]
then
	echo "${allofthem//$'\n'/ }"
	exit;
elif [ "$1" = "all" ]
then
	genres="${allofthem}"
fi

for genre in ${genres}
do
	partsqlfile="partition_${genre}.sql"
	partcheckfile="check_${genre}.sql"

	if [ ! -s "${partsqlfile}" ]
	then
		echo
		echo "## !! ${partsqlfile} does not exists or is empty"
		echo
	else
		if [ ! -s ${partcheckfile} ]
		then
			echo
			echo "## !! ${genre} check is empty"
			echo
		fi
		createdfunctions="$(grep -E '^[[:space:]]*CREATE[[:space:]]*FUNCTION' ${partsqlfile} |sed -r -e'/^[[:space:]]*CREATE[[:space:]]*FUNCTION/ s/^[[:space:]]*CREATE[[:space:]]*FUNCTION[[:space:]]*(.*)$/\1/' -e'/^[[:space:]]*$/d' | cut -d'(' -f1 | sort -u)"
		
		#echo "grep -E \"^[[:space:]]*FROM[[:space:]]+admin\.\" ${partcheckfile} | sed -r -e's/^[[:space:]]*FROM[[:space:]]+admin\.//g' | cut -d'(' -f1 | sort -u"
		checkedfunctions="$(grep -E "^[[:space:]]*FROM[[:space:]]*admin\." ${partcheckfile} | sed -r -e's/^[[:space:]]*FROM[[:space:]]*admin\./admin./g' -e'/^[[:space:]]*$/d' | cut -d'(' -f1 | sort -u)"
		
		#echo "functions :"
		#echo "${createdfunctions}"
		#echo "checked   :"
		#echo "${checkedfunctions}"
		res=$(diff -y --suppress-common-lines <(echo "${createdfunctions}") <(echo "${checkedfunctions}"))
		if [ "${res}" != "" ]
		then
			echo
			echo "mismatching checks in ${genre}:"
			echo "${res}"
			echo
		else
			echo "${genre} is complete"
		fi
	fi
done
