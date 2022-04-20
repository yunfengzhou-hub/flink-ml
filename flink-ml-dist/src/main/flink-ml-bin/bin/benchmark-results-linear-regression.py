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
import json
import numpy as np


def linear_regression(filename, x_field, y_field):
    def get_field(json, fieldnames):
        field = json
        for fieldname in fieldnames.split("."):
            field = field[fieldname]
        return field

    x_values = []
    y_values = []
    with open(filename, "r") as configfile:
        for _, config in json.loads(configfile.read()).items():
            x_values.append(get_field(config, x_field))
            y_values.append(get_field(config, y_field))

    P2 = np.polyfit(x_values, y_values, 1)
    p = np.poly1d(P2)
    y_pred = p(x_values)
    y_avg = sum(y_values)/len(y_values)
    SST = sum((y-y_avg)**2 for y in y_values)
    SSreg = sum((y_pred - y_avg)**2)
    R2 = SSreg/SST

    print(p)
    print("R^2 = " + str(R2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Conducts linear regression analysis on a list of benchmark results")
    parser.add_argument(
        "filename", help="Benchmark results file to conduct linear regression analysis.")

    field_grammar = "Use dot(.) to identify nested fields. For example, `inputData.paramMap.numValues`."
    parser.add_argument("x_field", help="Name of the independent field. " + field_grammar)
    parser.add_argument("y_field", help="Name of the dependent field. " + field_grammar)
    args = parser.parse_args()

    linear_regression(args.filename, args.x_field, args.y_field)
