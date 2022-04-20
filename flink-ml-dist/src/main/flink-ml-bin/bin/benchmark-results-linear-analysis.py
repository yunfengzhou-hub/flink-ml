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
import matplotlib.pyplot as plt
import numpy as np
import re


def linear_regression(filename, name_pattern, x_field, y_field):
    def get_field(json, fieldnames):
        field = json
        for fieldname in fieldnames.split("."):
            field = field[fieldname]
        return field

    x_values = []
    y_values = []
    with open(filename, "r") as configfile:
        for name, config in json.loads(configfile.read()).items():
            if not name_pattern.match(name):
                continue
            x_values.append(get_field(config, x_field))
            y_values.append(get_field(config, y_field))

    P2 = np.polyfit(x_values, y_values, 1)
    p = np.poly1d(P2)
    y_pred = p(x_values)
    y_avg = sum(y_values)/len(y_values)
    SST = sum((y-y_avg)**2 for y in y_values)
    SSreg = sum((y_pred - y_avg)**2)
    R2 = SSreg/SST

    print("y = {slope} x + {intercept}".format(
        slope=P2[0], intercept=P2[1]))
    print("R^2 = {R2}".format(R2=R2))

    plt.scatter(x_values, y_values)
    plt.plot(x_values, y_pred, color='g')
    plt.legend(["y = {slope:.2f} x + {intercept:.2f}".format(
        slope=P2[0], intercept=P2[1]), "R^2 = {R2:.2f}".format(R2=R2)])
    plt.xlabel(x_field)
    plt.ylabel(y_field)
    plt.title("Linear Regression of " + x_field + " Against " + y_field)
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Conducts linear regression analysis on a list of benchmark results")
    parser.add_argument(
        "filename", help="Benchmark results file to conduct linear regression analysis.")

    field_grammar = "Use dot(.) to identify nested fields. For example, `inputData.paramMap.numValues`."
    parser.add_argument(
        "x_field", help="Name of the independent field. " + field_grammar)
    parser.add_argument(
        "y_field", help="Name of the dependent field. " + field_grammar)
    parser.add_argument(
        "--pattern", help="Regex pattern of benchmark names. "
                          "Benchmarks whose names match this pattern will be selected for linear analysis. "
                          "If not set, all benchmarks would be selected.",
        default=".*")
    args = parser.parse_args()

    linear_regression(args.filename, re.compile(
        args.pattern), args.x_field, args.y_field)
