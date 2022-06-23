---
title: "Binary Classification Evaluator"
weight: 1
type: docs
aliases:
- /operators/evaluation/binaryclassificationevaluator.html


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

# Binary Classification Evaluator

Binary Classification Evaluator calculates the evaluation metrics for binary classification. The input data has `rawPrediction`, `label`, and an optional weight column. The `rawPrediction` can be of type double (binary 0/1 prediction, or probability of label 1) or of type vector (length-2 vector of raw predictions, scores, or label probabilities). The output may contain different metrics defined by the parameter `MetricsNames`.
## Input Columns

| Param name       | Type          | Default         | Description               |
| :--------------- | :------------ | :-------------- | :------------------------ |
| labelCol         | Number        | `"label"`       | The label of this entry   |
| rawPredictionCol | Vector/Number | `rawPrediction` | The raw prediction result |
| weightCol        | Number        | `null`          | The weight of this entry  |

## Output Columns

| Column name       | Type   | Description                                                  |
| ----------------- | ------ | ------------------------------------------------------------ |
| "areaUnderROC"    | Double | the area under the receiver operating characteristic (ROC) curve |
| "areaUnderPR"     | Double | the area under the precision-recall curve                    |
| "areaUnderLorenz" | Double | Kolmogorov-Smirnov, measures the ability of the model to separate positive and negative samples |
| "ks"              | Double | the area under the lorenz curve                              |

## Parameters

| Key              | Default                                                      | Type         | Required | Description                  |
| ---------------- | ------------------------------------------------------------ | ------------ | -------- | ---------------------------- |
| labelCol         | `"label"`                                                    | String       | no       | Label column name.           |
| weightCol        | `null`                                                       | String       | no       | Weight column name.          |
| rawPredictionCol | `"rawPrediction"`                                            | String       | no       | Raw prediction column name.  |
| metricsNames     | `[BinaryClassificationEvaluatorParams.AREA_UNDER_ROC, BinaryClassificationEvaluatorParams.AREA_UNDER_PR]` | String Array | no       | Names of the output metrics. |

## Examples

{{< tabs examples >}}

{{< tab "Java">}}

```java
import org.apache.flink.ml.evaluation.binaryclassification.BinaryClassificationEvaluator;
import org.apache.flink.ml.evaluation.binaryclassification.BinaryClassificationEvaluatorParams;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

// Generates input data.
DataStream<Row> inputStream =
  env.fromElements(
  Row.of(1.0, Vectors.dense(0.1, 0.9)),
  Row.of(1.0, Vectors.dense(0.2, 0.8)),
  Row.of(1.0, Vectors.dense(0.3, 0.7)),
  Row.of(0.0, Vectors.dense(0.25, 0.75)),
  Row.of(0.0, Vectors.dense(0.4, 0.6)),
  Row.of(1.0, Vectors.dense(0.35, 0.65)),
  Row.of(1.0, Vectors.dense(0.45, 0.55)),
  Row.of(0.0, Vectors.dense(0.6, 0.4)),
  Row.of(0.0, Vectors.dense(0.7, 0.3)),
  Row.of(1.0, Vectors.dense(0.65, 0.35)),
  Row.of(0.0, Vectors.dense(0.8, 0.2)),
  Row.of(1.0, Vectors.dense(0.9, 0.1)));
Table inputTable = tEnv.fromDataStream(inputStream).as("label", "rawPrediction");

// Creates a BinaryClassificationEvaluator object and initializes its parameters.
BinaryClassificationEvaluator evaluator =
  new BinaryClassificationEvaluator()
  .setMetricsNames(
  BinaryClassificationEvaluatorParams.AREA_UNDER_PR,
  BinaryClassificationEvaluatorParams.KS,
  BinaryClassificationEvaluatorParams.AREA_UNDER_ROC);

// Uses the BinaryClassificationEvaluator object for evaluations.
Table outputTable = evaluator.transform(inputTable)[0];

// Extracts and displays the results.
Row evaluationResult = outputTable.execute().collect().next();
System.out.printf(
  "Area under the precision-recall curve: %s\n",
  evaluationResult.getField(BinaryClassificationEvaluatorParams.AREA_UNDER_PR));
System.out.printf(
  "Area under the receiver operating characteristic curve: %s\n",
  evaluationResult.getField(BinaryClassificationEvaluatorParams.AREA_UNDER_ROC));
System.out.printf(
  "Kolmogorov-Smirnov value: %s\n",
  evaluationResult.getField(BinaryClassificationEvaluatorParams.KS));

```

{{< /tab>}}

{{< tab "Python">}}

```python
to be completed

```

{{< /tab>}}
{{< /tabs>}}







