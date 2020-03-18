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
#set SPARK_COMMAND, if not set already
if [ -z "${SPARK_COMMAND}" ]; then
  export SPARK_COMMAND=/spark/spark/bin/spark-submit
fi

#set APP_HOME, if not set already
if [ -z "${APP_HOME}" ]; then
  export APP_HOME="$(cd "`dirname "$0"`"/..; readlink -f .)"
fi

APP_CONF="${APP_HOME}/conf/bloom.properties"

if [ ! -f "${APP_CONF}" ]; then
    echo "****Invalid state => Properties not found:${APP_CONF}!"
    exit 1;
fi

function prop {
    grep "${1}" "${APP_CONF}"|cut -d'=' -f2
}

function email {
    mailbody="${1}"
    ${APP_HOME}/bin/sendemail.sh ${APP_CONF} "${mailbody}"
}

APP_JAR=`ls ${APP_HOME}/bloom-core*.jar`
# add config directory to classpath
CLASSPATH=''

i=0;
for f in "${APP_HOME}"/lib/*.jar; do
  if [ $i -eq 0 ]; then
        CLASSPATH="$f"
  else
        CLASSPATH="$CLASSPATH,$f";
  fi
  i=$((i + 1))
done


args=$@
configFile="BloomSparkHigh.conf";

#Prepare classpath- begin#
JAVA_CLASSPATH="${APP_JAR}:`echo ${CLASSPATH} | sed 's/,/:/g'`:`hadoop classpath`:`hbase classpath`"
WAREHOUSE="$(prop 'blaze.bloom.warehouse')"
INPUT_PATH=`echo $@ | grep -o -P '\-ip.*' | cut -d " " -f2`


if [ -d $INPUT_PATH ]; then
        echo "Input file is  a directory : " $INPUT_PATH
        echo "Please pass the input file for BLooMLauncher to process, exiting from BLooMLauncher... "
        exit 1
fi
APP_FILE="APP_`date +"%s"`"
filePathArg=" -fp ${APP_HOME}/${APP_FILE}"
finalargs="${@}${filePathArg}"

"${SPARK_COMMAND}" --verbose \
--name "BLOOM" \
--properties-file "$APP_HOME/conf/$configFile" \
--deploy-mode client \
--master yarn \
--jars "${CLASSPATH}" \
--conf "spark.local.dir=${WAREHOUSE}" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${APP_HOME}/conf/log4j.xml -XX:+UseParallelGC -XX:ParallelGCThreads=8 -XX:ConcGCThreads=2 -DAPP_HOME=${APP_HOME} -Djava.io.tmpdir=${WAREHOUSE}" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${APP_HOME}/conf/log4j.xml -XX:+UseParallelGC -XX:ParallelGCThreads=8 -XX:ConcGCThreads=3 -DAPP_HOME=${APP_HOME} -Dio.netty.leakDetection.level=advanced -Djava.io.tmpdir=${WAREHOUSE}" \
--class "BulkLoadDriver" "${APP_JAR}" ${finalargs}
sta=$?
echo "back to the launcher "
INPUT_TYPE=`echo $@ | grep -o -P '\-it.*' | cut -d " " -f2`
isArchive=`echo $@ | grep -o -P '\-ar.*' | cut -d " " -f2`
shopt -s nocasematch
s1='true'
s2=$isArchive
if [[ "$s1" == "$s2" ]]; then
 status="true"
else
 status="false"
fi

shopt -s nocasematch
csv1='csv'
csv2=$INPUT_TYPE
[[ "$csv1" == "$csv2" ]] && cs="csv" || cs="not"


if [ $status == "true" ]  && [ $cs == "csv" ]; then
 echo "Archival is chosen";
 sh $APP_HOME/bin/archive.sh $INPUT_PATH "${sta}" $APP_CONF
elif [ $cs == "csv" ]; then
 echo " Archive process not chosen"
else
 echo ""
fi

#email "testing email functionality"
if [ -f "${APP_HOME}/${APP_FILE}" ]; then
    mailContent=`cat ${APP_HOME}/${APP_FILE}`
    app_data=`${JAVA_HOME}/bin/java  -DJOB=velocity -DAPP_HOME=${APP_HOME} -classpath "${APP_JAR}:${JAVA_CLASSPATH}" com.americanexpress.bloom.util.VelocityUtils OBJ "JobDetails" "${APP_HOME}/${APP_FILE}"`
    hadoop fs -rm "${APP_HOME}/${APP_FILE}"
    email "${app_data}"
fi
exit "${sta}"
