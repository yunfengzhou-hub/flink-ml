---
title: "String Indexer"
weight: 1
type: docs
aliases:
- /operators/feature/stringindexer.html

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

# String Indexer / Index To String

String Indexer maps one or more columns (string/numerical value) of the input to one or more indexed output columns (integer value). The output indices of two data points are the same iff their corresponding input columns are the same. The indices are in [0, numDistinctValuesInThisColumn].

IndexToStringModel transforms input index column(s) to string column(s) using the model data computed by StringIndexer. It is a reverse operation of StringIndexerModel.
## Input Columns

| Param name | Type          | Default | Description                            |
| :--------- | :------------ | :------ | :------------------------------------- |
| inputCols  | Number/String | `null`  | string/numerical values to be indexed. |

## Output Columns

| Param name | Type   | Default | Description                         |
| :--------- | :----- | :------ | :---------------------------------- |
| outputCols | Double | `null`  | Indices of string/numerical values. |

## Parameters

Below are the parameters required by `StringIndexerModel`.

| Key           | Default                          | Type   | Required | Description                         |
| ------------- | -------------------------------- | ------ | -------- | ----------------------------------- |
| inputCols     | `null`                           | String | yes      | Input column names.                 |
| outputCols    | `null`                           | String | yes      | Output column names.                |
| handleInvalid | `HasHandleInvalid.ERROR_INVALID` | String | No       | Strategy to handle invalid entries. |

`StringIndexer` needs parameters above and also below.

| Key             | Default                               | Type   | Required | Description                          |
| --------------- | ------------------------------------- | ------ | -------- | ------------------------------------ |
| stringOrderType | `StringIndexerParams.ARBITRARY_ORDER` | String | no       | How to order strings of each column. |

## Examples

{{< tabs examples >}}

{{< tab "Java">}}

```java
import org.apache.flink.ml.feature.stringindexer.StringIndexer;
import org.apache.flink.ml.feature.stringindexer.StringIndexerModel;
import org.apache.flink.ml.feature.stringindexer.StringIndexerParams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

// Generates input training and prediction data.
DataStream<Row> trainStream =
  env.fromElements(
  Row.of("a", 1.0),
  Row.of("b", 1.0),
  Row.of("b", 2.0),
  Row.of("c", 0.0),
  Row.of("d", 2.0),
  Row.of("a", 2.0),
  Row.of("b", 2.0),
  Row.of("b", -1.0),
  Row.of("a", -1.0),
  Row.of("c", -1.0));
Table trainTable = tEnv.fromDataStream(trainStream).as("inputCol1", "inputCol2");

DataStream<Row> predictStream =
  env.fromElements(Row.of("a", 2.0), Row.of("b", 1.0), Row.of("c", 2.0));
Table predictTable = tEnv.fromDataStream(predictStream).as("inputCol1", "inputCol2");

// Creates a StringIndexer object and initializes its parameters.
StringIndexer stringIndexer =
  new StringIndexer()
  .setStringOrderType(StringIndexerParams.ALPHABET_ASC_ORDER)
  .setInputCols("inputCol1", "inputCol2")
  .setOutputCols("outputCol1", "outputCol2");

// Trains the StringIndexer Model.
StringIndexerModel model = stringIndexer.fit(trainTable);

// Uses the StringIndexer Model for predictions.
Table outputTable = model.transform(predictTable)[0];

// Extracts and displays the results.
for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
  Row row = it.next();

  Object[] inputValues = new Object[stringIndexer.getInputCols().length];
  double[] outputValues = new double[stringIndexer.getInputCols().length];
  for (int i = 0; i < inputValues.length; i++) {
    inputValues[i] = row.getField(stringIndexer.getInputCols()[i]);
    outputValues[i] = (double) row.getField(stringIndexer.getOutputCols()[i]);
  }

  System.out.printf(
    "Input Values: %s \tOutput Values: %s\n",
    Arrays.toString(inputValues), Arrays.toString(outputValues));
}

```

{{< /tab>}}

{{< tab "Python">}}

```python
to be completed

```

{{< /tab>}}
{{< /tabs>}}







