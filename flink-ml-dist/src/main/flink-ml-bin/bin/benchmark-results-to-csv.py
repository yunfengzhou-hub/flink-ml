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
import csv
import json


def json2csv(jsonfilename, csvfilename):
    def get_field(json, fieldnames):
        field = json
        for fieldname in fieldnames:
            if isinstance(field, dict) and fieldname in field:
                field = field[fieldname]
            else:
                return ""
        if not field:
            return ""
        return field

    def add_all_fields(field_names, json_obj, prefix):
        for key, value in json_obj.items():
            new_prefix = prefix + [key]
            if isinstance(value, dict):
                add_all_fields(field_names, value, new_prefix)
            else:
                if new_prefix not in field_names:
                    field_names.append(new_prefix)

    def get_field_names(json_list):
        field_names = []
        for json_obj in json_list:
            add_all_fields(field_names, json_obj, [])
        return sorted(field_names)

    with open(jsonfilename, "r") as jsonfile:
        results = json.loads(jsonfile.read())

    results_list = []
    for key, value in results.items():
        value["name"] = key
        results_list.append(value)

    field_names = get_field_names(results_list)
    rows = [[get_field(result, field) for field in field_names]
            for result in results_list]

    if csvfilename:
        with open(csvfilename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([".".join(x) for x in field_names])
            csvwriter.writerows(rows)
        print("Converted benchmark results saved in " + csvfilename + ".")
    else:
        print(",".join([".".join(x) for x in field_names]))
        for row in rows:
            print(",".join(str(x) for x in row))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Converts the benchmark results json file to csv file.")
    parser.add_argument(
        "inputfilename", help="Json file to acquire the benchmark results.")
    parser.add_argument(
        "--output-file", help="File to store converted csv results. If not set, results would be printed to terminal.")
    args = parser.parse_args()

    json2csv(args.inputfilename, args.output_file)
