#!/bin/bash

INPUT_PATH=$1
JOB_STATUS=$2
APP_CONF=$3

ARCHIVE_PATH=` grep "blaze.bloom.archive.path" $APP_CONF | cut -d'=' -f2`
ARCHIVE_PATH=${ARCHIVE_PATH%?}

ARCHIVE_MAX_DAYS=` grep "blaze.bloom.archive.days" $APP_CONF | cut -d'=' -f2`
ARCHIVE_MAX_DAYS=${ARCHIVE_MAX_DAYS%?}

echo "INPUT_PATH is : " $INPUT_PATH
echo "ARCHIVE_PATH is : " $ARCHIVE_PATH
echo "ARCHIVE_MAX_DAYS is : " $ARCHIVE_MAX_DAYS


if [[ -f $INPUT_PATH ]]; then
    echo "$INPUT_PATH is a file"
else
    echo "$INPUT_PATH is not valid"
    exit 1
fi

if [[ $JOB_STATUS -eq "0" ]]; then
   JOB_STATUS="COMPLETED"
else
    JOB_STATUS="FAILED"
fi

## Extracting file names from input path

FILENAMES=`echo $INPUT_PATH | rev | cut -d/ -f1 | rev`
FILENAME=`echo $FILENAMES | rev | cut -d. -f2 | rev`

## Calculating current timestamp to append to archive folder
runDate=`date +%Y%m%d`
runTime=`date +%H%M%S`
ARCHIVE_FILENAME="$FILENAME""_""$runDate""_""$runTime""_""$JOB_STATUS"".dat"

if [ ! -d "$ARCHIVE_PATH" ]; then
  mkdir -p $ARCHIVE_PATH
fi

echo "${ARCHIVE_PATH}""${ARCHIVE_FILENAME}"

hadoop dfs -cp $INPUT_PATH "${ARCHIVE_PATH}""${ARCHIVE_FILENAME}"
echo "Archive process has been completed"

count=`find $ARCHIVE_PATH -mindepth 1 -mtime $ARCHIVE_MAX_DAYS | wc -l`
echo "Older than $ARCHIVE_MAX_DAYS Days files count is: " $count

find $ARCHIVE_PATH -mindepth 1 -mtime $ARCHIVE_MAX_DAYS -exec rm -rf {} \;
