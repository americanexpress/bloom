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

#!/bin/bash
## ---------------------------------------------------------------------------------------------- ##
## Check the No. of Paramters
## ---------------------------------------------------------------------------------------------- ##
usage() {
        echo "`date +%Y%m%d%H%M%S`: Usage: $0 <ENV> <body> "1>&2;
        exit 1;
}
if [ $# -ne 2 ]
then
        usage;
fi;

mbl_config=$1;
echo "config !!!"${mbl_config}
if [ -z "${APP_HOME}" ]; then
  export APP_HOME="$(cd "`dirname "$0"`"/..; readlink -f .)"
fi
echo "APP_HOME $APP_HOME"

echo "sending email with the job details !!!!"
mailSubject=`grep "blaze.bloom.email.subject=" ${mbl_config} | rev | cut -d= -f1 | rev`
to=`grep "bloom.email.recipients=" ${mbl_config} | rev | cut -d= -f1 | rev`
#from=`grep "emailSender=" ${a2dt_config} | rev | cut -d= -f1 | rev`

mailContent="$2"
(
#echo From: $from
echo To: $to
echo Subject: "$mailSubject"
echo "Mime-Version: 1.0"
echo 'Content-Type: multipart/mixed; boundary="GvXjxJ+pjyke8COw"'
echo ""
echo "--GvXjxJ+pjyke8COw"
echo "Content-Type: text/html"
echo "Content-Disposition: inline"
echo ""
echo "${mailContent}"
echo ""
) | /usr/sbin/sendmail -t
