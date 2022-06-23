---
title: "One Hot Encoder"
weight: 1
type: docs
aliases:
- /operators/feature/onehotencoder.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# One Hot Encoder

One-hot encoding maps a categorical feature, represented as a label index, to a
binary vector with at most a single one-value indicating the presence of a
specific feature value from among the set of all feature values. This encoding
allows algorithms that expect continuous features, such as Logistic Regression,
to use categorical features.

OneHotEncoder can transform multiple columns, returning a one-hot-encoded
output vector column for each input column.

## Input Columns

| Param name | Type    | Default | Description |
| :--------- | :------ | :------ | :---------- |
| inputCols  | Integer | `null`  | Label index |

## Output Columns

| Param name | Type   | Default | Description           |
| :--------- | :----- | :------ | :-------------------- |
| outputCols | Vector | `null`  | Encoded binary vector |

## Parameters

| Key           | Default                          | Type    | Required | Description                                                  |
| ------------- | -------------------------------- | ------- | -------- | ------------------------------------------------------------ |
| inputCols     | `null`                           | String  | yes      | Input column names.                                          |
| outputCols    | `null`                           | String  | yes      | Output column names.                                         |
| handleInvalid | `HasHandleInvalid.ERROR_INVALID` | String  | No       | Strategy to handle invalid entries. Supported values: `HasHandleInvalid.ERROR_INVALID`, `HasHandleInvalid.SKIP_INVALID` |
| dropLast      | `true`                           | Boolean | no       | Whether to drop the last category.                           |

## Examples

{{< tabs examples >}}

{{< tab "Java">}}
```java
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

// Generates input training and prediction data.
DataStream<Row> trainStream =
  env.fromElements(Row.of(0.0), Row.of(1.0), Row.of(2.0), Row.of(0.0));
Table trainTable = tEnv.fromDataStream(trainStream).as("input");

DataStream<Row> predictStream = env.fromElements(Row.of(0.0), Row.of(1.0), Row.of(2.0));
Table predictTable = tEnv.fromDataStream(predictStream).as("input");

// Creates a OneHotEncoder object and initializes its parameters.
OneHotEncoder oneHotEncoder =
  new OneHotEncoder().setInputCols("input").setOutputCols("output");

// Trains the OneHotEncoder Model.
OneHotEncoderModel model = oneHotEncoder.fit(trainTable);

// Uses the OneHotEncoder Model for predictions.
Table outputTable = model.transform(predictTable)[0];

// Extracts and displays the results.
for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
  Row row = it.next();
  Double inputValue = (Double) row.getField(oneHotEncoder.getInputCols()[0]);
  SparseVector outputValue =
    (SparseVector) row.getField(oneHotEncoder.getOutputCols()[0]);
  System.out.printf("Input Value: %s\tOutput Value: %s\n", inputValue, outputValue);
}
```
{{< /tab>}}

{{< tab "Python">}}
```python
from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Table, DataTypes

from pyflink.ml.lib.feature.onehotencoder import OneHotEncoder, OneHotEncoderModel

# create a new StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# load flink ml jar
env.add_jars("file:///{path}/statefun-flink-core-3.1.0.jar", "file:///{path}/flink-ml-uber-{version}.jar")

# create a StreamTableEnvironment
t_env = StreamTableEnvironment.create(env)

train_table = t_env.from_elements(
    [Row(0.0), Row(1.0), Row(2.0), Row(0.0)],
    DataTypes.ROW([
        DataTypes.FIELD("input", DataTypes.DOUBLE())
    ]))

predict_table = t_env.from_elements(
    [Row(0.0), Row(1.0), Row(2.0)],
    DataTypes.ROW([
        DataTypes.FIELD("input", DataTypes.DOUBLE())
    ]))

estimator = OneHotEncoder().set_input_cols('input').set_output_cols('output')
model = estimator.fit(train_table)
output_table = model.transform(predict_table)[0]

output_table.execute().print()

# output
# +----+--------------------------------+--------------------------------+
# | op |                          input |                         output |
# +----+--------------------------------+--------------------------------+
# | +I |                            0.0 |                (2, [0], [1.0]) |
# | +I |                            1.0 |                (2, [1], [1.0]) |
# | +I |                            2.0 |                    (2, [], []) |
# +----+--------------------------------+--------------------------------+

```
{{< /tab>}}
{{< /tabs>}}







