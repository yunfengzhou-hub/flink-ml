#!/usr/bin/env bash
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

current_path=$(pwd)
flink_ml_bin_path="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
flink_ml_root_path="$(dirname "$flink_ml_bin_path")"

# Checks flink command.
flink_cmd="$FLINK_HOME/bin/flink"
if ! command -v $flink_cmd &> /dev/null; then
    echo "$flink_cmd: command not found. Please make sure you have installed Flink and configured Flink in your environment path."
    exit 1
fi

# Checks flink version.
expected_version="1.14"
actual_version=`$FLINK_HOME/bin/flink --version | cut -d" " -f2 | tr -d ","`
unsorted_versions="${expected_version}\n${actual_version}\n"
sorted_versions=`printf ${unsorted_versions} | sort -V`
unsorted_versions=`printf ${unsorted_versions}`
if [ "${unsorted_versions}" != "${sorted_versions}" ]; then
    echo "$flink_cmd $expected_version or a higher version is required, but found $actual_version"
    exit 1
fi

# Creates fat jar containing all flink ml dependencies to be submitted.
cd $flink_ml_root_path/lib
flink_ml_fat_jar_name="flink-ml-fat.jar"
if ! [ -f $flink_ml_fat_jar_name ]; then
    echo "Creating fat jar containing all flink ml dependencies to be submitted."
    mkdir tmp
    cd tmp
    for entry in "$flink_ml_root_path"/lib/*; do
      if [[ $entry == *.jar ]]; then
          unzip -quo $entry
      fi
    done
    cd ..
    jar -cf flink-ml-fat.jar -C tmp .
    rm -rf tmp
fi
cd $current_path

# Submits benchmark flink job.
$FLINK_HOME/bin/flink run -c org.apache.flink.ml.benchmark.Benchmark $flink_ml_root_path/lib/$flink_ml_fat_jar_name ${@:1}
