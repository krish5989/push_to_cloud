#!/bin/ksh
#############################################################################
# This script calls datafactory REST service to push files to Google cloud
#
# $Description: This script pushes data from LINUX file system to G-Cloud
# $Source: 
# $Revision: 0.2
# $modified by: 
#############################################################################

. /opt/hd/etc/set_env
. /srv/hadoop/mm/sz/cdn_pogcanada_cloud/utils/pog_canada_cloud_env.sh
export mailer="${UTIL_DIR}/thd_mailer.ksh"

log() 
{
	echo -e "INFO | `date +'%Y/%m/%d %H:%M:%S'` | $1 "
}

error() 
{
	echo -e "ERROR | `date +'%Y/%m/%d %H:%M:%S'` | $1 " > ${errfile}
}

function error_exit
{
    rc=`echo "$1" | tr -cd [:digit:]`
    shift
    if [ "X${rc}" = "X1" ]; then
    # Page if RC=1
    msg="POGCANADA datafactory job - $LCP, Completed with error while pushing data to cloud"
        
    # thd_logger 
    log "Paging Support: Message <$msg>"
        
        if [ $LCP = "PR" ]; then
        	${mailer} -f "POGCANADA_noreply@homedepot.com" -t "merch_IT_OPD@homedepot.com" -s "${msg}" -p High -bt "${errfile}"
        else
        	${mailer} -f "POGCANADA_noreply@homedepot.com" -t "merch_IT_OPD@homedepot.com" -c "krishnan_gopalakrishnan@homedepot.com" -s "${msg}" -p High -bt "${errfile}"
        fi
    fi
    exit ${rc}
}    

echo "1"
echo $LOG_DIR
#create log directory
if [ ! -d $LOG_DIR ]; then
echo "In"
	mkdir -p $LOG_DIR
	chmod -R 750 $LOG_DIR
fi

#determine datafactory URL based on LCP
if [ $LCP == "PR" ]; 
then
	HOSTNAME="bigdata.homedepot.com"
	SA_TOKEN="token"
else
	HOSTNAME="bigdata-qa.homedepot.com"
	SA_TOKEN="token"
fi

echo "HOST: "$HOSTNAME

#datafactory service URL
DF_URL=http://$HOSTNAME/DataFactory/rs/DataFactoryService/dataFactoryController
echo "URL: "$DF_URL

#metadata location 
if [ ${LCP} == "PR" ];
then
META_FILE_LOC="${UTIL_DIR}/pr_metadata"
else 
META_FILE_LOC="${UTIL_DIR}/qa_metadata"
fi

echo "Metadata files location: "$META_FILE_LOC

#Check if there are files to process in META_FILE_LOC
if [ -d $META_FILE_LOC ]; then
	FILES=(${META_FILE_LOC}/*)

	#check if there are files to process

	if [ ${#FILES[@]} -eq 0 ]; then 
		#no files to process - exit script successfully add TS
		echo "COMPLETED: NO FILES TO PROCESS"
		exit 0
	fi
	
	for metaFile in $FILES
	do
		echo "Processing file: "$metaFile
		
		#get job name - DO NOT CHANGE INDENTATION
		#check if fails
		echo "Before Job Name"
		JOBNAME="$(cat $metaFile | python -c 'import sys, json 
print(json.load(sys.stdin)["name"])')"
		if [ $? -ne 0 ]; then
			echo " Unable to parse JSON metadata "
			exit 1
		fi
		echo "Running job for :"$JOBNAME 
		
		LOGNAME=${LOG_DIR}/${JOBNAME}_$(date +%Y%m%d_%H%M%S).log
		echo "log directory : "$LOGNAME
		
		#execute call to datafactory service 
		echo "Running job for :"$JOBNAME >> $LOGNAME		
		
		RESULT=`curl -X POST -d @$metaFile -H "Content-Type: application/json" -H "THDService-Auth: ${SA_TOKEN}" $DF_URL 2>/dev/null` 
		
		echo $RESULT 
		JOBID="$(echo $RESULT | python -c 'import sys, json 
print(json.load(sys.stdin)["jobId"])')"
		echo "job ID: "$JOBID

		#check job status - options RUNNING, FINISHED, FAILED, CANCELED, PAUSED, QUEUED
		STATUS_URL=http://$HOSTNAME/DataFactory/rs/DataFactoryService/getJobInfo?jobId=$JOBID
		JOBSTATUS=`curl -X GET -H "Content-Type: application/json" -H "THDService-Auth: ${SA_TOKEN}" $STATUS_URL 2>/dev/null`

		echo $JOBSTATUS
		STATUS="$(echo $JOBSTATUS | python -c 'import sys, json 
print(json.load(sys.stdin)["status"])')"
		echo "job STATUS: "$STATUS
		#CUSTOMIZE JOB STATUS HANDLING AS DESIRED, BELOW IS AN EXAMPLE
		#status is finished then job was successful and data is loaded to cloud		
		#status is FAILED, CANCELED, PAUSED exit 1		
		#status is RUNNING, QUEUED then keep checking status till job is finished/fails	
    done
fi
		#Checking JOB STATUS for DataFactory push
		#CHECK FOR 'FINISHED' STATUS
                while [ ${STATUS} != 'FINISHED' ]
                do
                if [[ ${STATUS} == 'FAILED' || ${STATUS} == 'CANCELED' ]]; then
                   echo "Process $JOBNAME Failed" >> $LOGNAME
                   echo "Data Factory process for $JOBNAME Failed.Check $JOB_LOG for details" >> $LOGNAME
                   echo "Data factory process for $JOBNAME Failed.Check $JOB_LOG for details" >> ${errfile}
                   echo "Data Factory process for $JOBNAME exiting with status 1" >> $LOGNAME
                   echo "Data Factory process for $JOBNAME exiting with status 1" >> ${errfile}
                   error_exit 1
                fi
                echo "Checking Status again for the process "$JOBNAME >> $LOGNAME
        		sleep 60
       			JOBSTATUS=`curl -X GET -H "Content-Type: application/json" -H "THDService-Auth: ${SA_TOKEN}" $STATUS_URL 2>/dev/null`
        		echo $JOBSTATUS >> $LOGNAME
        		echo "" >> $LOGNAME
        		STATUS="$(echo $JOBSTATUS | python -c 'import sys, json
print(json.load(sys.stdin)["status"])')"
        		echo "job STATUS: "$STATUS >> $LOGNAME
           done
        
        echo "Data Factory process for $JOBNAME completed successfully" >> $LOGNAME
		echo "Data Factory process Completed : All Files were processed" >> $LOGNAME
		
		job_result=$(grep -i "Data Factory process Completed : All Files were processed" ${LOGNAME} | wc -l )		
		if [ ${job_result} -eq 1 ]; then
		${mailer} -f "from_id" -t "to_id" -s "POGCANADA datafactory job -${LCP}, completed successfully" -a "${LOGNAME}"
		else
		${mailer} -f "from_id" -t "to_id" -s "POGCANADA datafactory job -${LCP}, status could not be traced please check datafactory pool" -a "${LOGNAME}"
		fi
		
exit 0
