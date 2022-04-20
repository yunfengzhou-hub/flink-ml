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
import argparse
import copy
import json


def generate_batch_benchmark_config(input_filename, benchmark_name, parameter_name, parameter_values, output_filename):
    with open(input_filename, "r") as input_file:
        input_config = json.loads(input_file.read())[benchmark_name]

    output_configs = {"version": 1}
    for paramvalue in parameter_values:
        config = copy.deepcopy(input_config)
        field = config
        for fieldname in parameter_name.split(".")[:-1]:
            field = field[fieldname]
        field[parameter_name.split(".")[-1]] = paramvalue
        output_configs[benchmark_name + "-" + str(paramvalue)] = config

    if output_filename:
        with open(output_filename, "w") as outputfile:
            outputfile.write(json.dumps(output_configs, indent=4))
        print("Batch benchmark configuration generated and saved in " + output_filename + ".")
    else:
        print(output_configs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates a bunch of benchmark configurations by changing an independent parameter variable.")
    parser.add_argument(
        "input_filename", help="Benchmark configuration file to acquire the default parameters.")
    parser.add_argument(
        "benchmark_name", help="Benchmark name to acquire the default parameters.")
    parser.add_argument(
        "parameter_name", help="Independent parameter name whose value would be changed. ")
    parser.add_argument("parameter_values", nargs="+", type=float,
                        help="A list of values to be set on the parameter.")
    parser.add_argument(
        "--output-file", help="Output file to store generated benchmark configs. "
                              "If not set, generated configs would be printed to terminal.")
    args = parser.parse_args()

    generate_batch_benchmark_config(
        args.input_filename, args.benchmark_name, args.parameter_name, args.parameter_values, args.output_file)
