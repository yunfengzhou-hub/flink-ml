---
title: "Kmeans"
type: docs
aliases:
- /operators/clustering/kmeans.html
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

# K-means

K-means is a commonly-used clustering algorithm. It groups given data points
into a predefined number of clusters.

## Input Columns

| Param name  | Type   | Default      | Description    |
| :---------- | :----- | :----------- | :------------- |
| featuresCol | Vector | `"features"` | Feature vector |

## Output Columns

| Param name    | Type    | Default        | Description              |
| :------------ | :------ | :------------- | :----------------------- |
| predictionCol | Integer | `"prediction"` | Predicted cluster center |

## Parameters

Below are the parameters required by `KMeansModel`.

| Key             | Default                         | Type    | Required | Description                                                  |
| --------------- | ------------------------------- | ------- | -------- | ------------------------------------------------------------ |
| distanceMeasure | `EuclideanDistanceMeasure.NAME` | String  | no       | Distance measure. Supported values: `EuclideanDistanceMeasure.NAME` |
| featuresCol     | `"features"`                    | String  | no       | Features column name.                                        |
| predictionCol   | `"prediction"`                  | String  | no       | Prediction column name.                                      |
| k               | `2`                             | Integer | no       | The max number of clusters to create.                        |

`KMeans` needs parameters above and also below.

| Key      | Default    | Type    | Required | Description                                                |
| -------- | ---------- | ------- | -------- | ---------------------------------------------------------- |
| initMode | `"random"` | String  | no       | The initialization algorithm. Supported options: 'random'. |
| seed     | `null`     | Long    | no       | The random seed.                                           |
| maxIter  | `20`       | Integer | no       | Maximum number of iterations.                              |

## Examples

{{< tabs examples >}}

{{< tab "Java">}}
```java
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

// Generates input data.
DataStream<DenseVector> inputStream =
  env.fromElements(
  Vectors.dense(0.0, 0.0),
  Vectors.dense(0.0, 0.3),
  Vectors.dense(0.3, 0.0),
  Vectors.dense(9.0, 0.0),
  Vectors.dense(9.0, 0.6),
  Vectors.dense(9.6, 0.0));
Table inputTable = tEnv.fromDataStream(inputStream).as("features");

// Creates a K-means object and initializes its parameters.
KMeans kmeans = new KMeans().setK(2).setSeed(1L);

// Trains the K-means Model.
KMeansModel kmeansModel = kmeans.fit(inputTable);

// Uses the K-means Model for predictions.
Table outputTable = kmeansModel.transform(inputTable)[0];

// Extracts and displays the results.
for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
  Row row = it.next();
  DenseVector features = (DenseVector) row.getField(kmeans.getFeaturesCol());
  int clusterId = (Integer) row.getField(kmeans.getPredictionCol());
  System.out.printf("Features: %s \tCluster ID: %s\n", features, clusterId);
}

```
{{< /tab>}}

{{< tab "Python">}}
```python
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from pyflink.ml.core.linalg import Vectors, DenseVectorTypeInfo
from pyflink.ml.lib.clustering.kmeans import KMeans

# create a new StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# load flink ml jar
env.add_jars("file:///{path}/statefun-flink-core-3.1.0.jar", "file:///{path}/flink-ml-uber-{version}.jar")

# create a StreamTableEnvironment
t_env = StreamTableEnvironment.create(env)

data_table = t_env.from_data_stream(
    env.from_collection([
        (Vectors.dense([0.0, 0.0]),),
        (Vectors.dense([0.0, 0.3]),),
        (Vectors.dense([0.3, 3.0]),),
        (Vectors.dense([9.0, 0.0]),),
        (Vectors.dense([9.0, 0.6]),),
        (Vectors.dense([9.6, 0.0]),),
    ],
        type_info=Types.ROW_NAMED(
            ['features'],
            [DenseVectorTypeInfo()])))

kmeans = KMeans().set_k(2).set_seed(1)

model = kmeans.fit(data_table)

output = model.transform(data_table)[0]

output.execute().print()

# output
# +----+--------------------------------+-------------+
# | op |                       features |  prediction |
# +----+--------------------------------+-------------+
# | +I |                     [9.0, 0.0] |           1 |
# | +I |                     [0.0, 0.0] |           0 |
# | +I |                     [9.0, 0.6] |           1 |
# | +I |                     [0.3, 3.0] |           0 |
# | +I |                     [0.0, 0.3] |           0 |
# | +I |                     [9.6, 0.0] |           1 |
# +----+--------------------------------+-------------+
```
{{< /tab>}}
{{< /tabs>}}
