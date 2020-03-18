# Copyright 2020 American Express Travel Related Services Company, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.

#Setup begin
export APP_HOME="$(cd "`dirname "$0"`"/..; readlink -f .)"

#check if any file is owned by pmcprod then change permission to group
function changepermissions {
if [ "$UID" = "2600" ]; then
    for name in $(find "${1}" -user 2600 ) ; do
        echo "Changing permission: `ls -ltr $name`"
        chown $UID:70084 "${name}"
        chmod 770 "${name}"
    done
fi
}
###### LOGS
runDate=`date +%Y%m%d`

TABLE_NAME=`echo $@ | grep -o -P '\-tn.*' | cut -d " " -f2`
INPUT_PATH=`echo $@ | grep -o -P '\-ip.*' | cut -d " " -f2`
REFRESH_TYPE=`echo $@ | grep -o -P '\-rt.*' | cut -d " " -f2`
INPUT_TYPE=`echo $@ | grep -o -P '\-it.*' | cut -d " " -f2`
TABLE_TYPE=`echo $@ | grep -o -P '\-tt.*' | cut -d " " -f2`
ISARCHIVE=`echo $@ | grep -o -P '\-ar.*' | cut -d " " -f2`


shopt -s nocasematch
s1='true'
s2=$ISARCHIVE
[[ "$s1" == "$s2" ]] && ISARCHIVE="true" || ISARCHIVE="false"

echo "Table name is : "$TABLE_NAME
echo "Input path is : " $INPUT_PATH
echo "Refresh type is : " $REFRESH_TYPE
echo "Input type is : "$INPUT_TYPE
echo "Table type is : "$TABLE_TYPE

#Assigning the constants
MAXDEPTH=1
NAME_PATTERN='*.*'
listOfFiles=`ls -lrt $INPUT_PATH//*.* | awk '{print $9}'`

if [ ! -d $INPUT_PATH ]; then

        echo " Input path is not a directory : " $INPUT_PATH
        exit 1
fi



atleastOneJobFailed=0
for absfile in $(find "${INPUT_PATH}" -maxdepth "${MAXDEPTH}" -type f -name "${NAME_PATTERN}");
do
   echo "Started processing file is : " $absfile
   SUB='.ctl'
   if [[ "$absfile" != *"$SUB"* ]]; then
        FILENAMEARGS="${@/${INPUT_PATH}/$absfile}"
        bash "${APP_HOME}/bin/BLooMLauncher.sh" $FILENAMEARGS
        XFORMATION_EXIT_CODE=$?
        if [ ${XFORMATION_EXIT_CODE} -ne 0 ]; then
         atleastOneJobFailed=1
         echo "Process failed for the file is : " $absfile
        else
         echo "File has been processed  successfully : " $absfile
       fi
   fi
done
exit "${atleastOneJobFailed}"