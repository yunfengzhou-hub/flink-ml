#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This scripts runs a given test in a loop until it fails.
# All the logs are redirected to a log file and will be deleted if the test has passed.

if [ $# -ne 1 ];then
	echo "USAGE: run_until_fail.sh TEST_PATH"
	exit
fi
success=0
round=0
while [ $round -le 500 ]
do
	echo "Running tests: Run $round"
  log=test_log_$1_$round.log
	mvn test -Dtest=$1 -Dcheckstyle.skip -nsu 2>&1 > $log
	success=$?
	if [ $success == 0 ]; then
		rm $log
	else
		echo "\nTest finished with return code $success."
		echo "Check log at $log."
		echo "======================================"
		cat $log
		echo "======================================"
		exit 1
	fi
	round=$(($round + 1))
done;
