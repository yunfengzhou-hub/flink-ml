# Flink ML Benchmark Getting Started

This document provides instructions on how to run benchmarks on Flink ML's
stages in a Linux/MacOS environment.

## Prerequisites

### Install Flink

Please make sure Flink 1.14 or higher version has been installed in your local
environment. You can refer to the [local
installation](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/local_installation/)
instruction on Flink's document website for how to achieve this.

### Set Up Flink Environment Variables

After having installed Flink, please register `$FLINK_HOME` as an environment
variable into your local environment.

```bash
cd ${path_to_flink}
export FLINK_HOME=`pwd`
```

Then please run the following command. If this command returns 1.14.0 or a
higher version, then it means that the required Flink environment has been
successfully installed and registered in your local environment.

```bash
$FLINK_HOME/bin/flink --version
```

[//]: # (TODO: Add instructions to download binary distribution when release is
    available)
### Build Flink ML library

In order to use Flink ML's CLI you need to have the latest binary distribution
of Flink ML. You can acquire the distribution by building Flink ML's source code
locally with the following command.

```bash
cd ${path_to_flink_ml}
mvn clean package -DskipTests
cd ./flink-ml-dist/target/flink-ml-*-bin/flink-ml*/
```

### Start Flink Cluster

Please start a Flink standalone session in your local environment with the
following command.

```bash
$FLINK_HOME/bin/start-cluster.sh
```

You should be able to navigate to the web UI at
[localhost:8081](http://localhost:8081/) to view the Flink dashboard and see
that the cluster is up and running.

## Run Benchmark Example

In Flink ML's binary distribution's folder, execute the following command to run
an example benchmark.

```bash
./bin/flink-ml-benchmark.sh ./examples/benchmark-conf.json --output-file results.json
```

You will notice that some Flink job is submitted to your Flink cluster, and the
following information is printed out in your terminal. This means that you have
successfully executed a benchmark on `KMeansModel`.

```
Creating fat jar containing all flink ml dependencies to be submitted.
Job has been submitted with JobID a5d8868d808eecfb357eb904c961c3bf
Program execution finished
Job with JobID a5d8868d808eecfb357eb904c961c3bf has finished.
Job Runtime: 897 ms
Accumulator Results: 
- numElements (java.lang.Long): 10000


{
  "name" : "KMeansModel-1",
  "totalTimeMs" : 897.0,
  "inputRecordNum" : 10000,
  "inputThroughput" : 11148.272017837235,
  "outputRecordNum" : 10000,
  "outputThroughput" : 11148.272017837235
}
```

The command above would save the results into `results.json` as below.

```json
[ {
  "name" : "KMeansModel-1",
  "totalTimeMs" : 897.0,
  "inputRecordNum" : 10000,
  "inputThroughput" : 11148.272017837235,
  "outputRecordNum" : 10000,
  "outputThroughput" : 11148.272017837235
} ]
```

## Customize Benchmark Configuration

`flink-ml-benchmark.sh` parses benchmarks to be executed according to the input
configuration file, like `./examples/benchmark-example-conf.json`. It can also
parse your custom configuration file so long as it contains a JSON object in the
following format.

- The file should contain the following as the metadata of the JSON object.
  - `"version"`: The version of the json format. Currently its value must be 1.
- Keys in the JSON object, except `"version"`, are regarded as the names of the
  benchmarks.
- The value of each benchmark name should be a JSON object containing the
  following keys.
  - `"stage"`: The stage to be benchmarked.
  - `"inputs"`: The input data of the stage to be benchmarked.
  - `"modelData"`(Optional): The model data of the stage to be benchmarked, if
    the stage is a `Model` and needs to have its model data explicitly set.
- The value of `"stage"`, `"inputs"` or `"modelData"` should be a JSON object
  containing the following keys.
  - `"className"`: The full classpath of a `WithParams` subclass. For `"stage"`,
    the class should be a subclass of `Stage`. For `"inputs"` or `"modelData"`,
    the class should be a subclass of `DataGenerator`.
  - `"paramMap"`: A JSON object containing the parameters related to the
    specific `Stage` or `DataGenerator`.

Combining the format requirements above, the example configuration in
`./examples/benchmark-example-conf.json` is as follows.

```json
{
  "version": 1,
  "KMeansModel-1": {
    "stage": {
      "className": "org.apache.flink.ml.clustering.kmeans.KMeansModel",
      "paramMap": {
        "featuresCol": "features",
        "k": 2,
        "distanceMeasure": "euclidean",
        "predictionCol": "prediction"
      }
    },
    "modelData":  {
      "className": "org.apache.flink.ml.benchmark.data.clustering.KMeansModelDataGenerator",
      "paramMap": {
        "seed": null,
        "arraySize": 2,
        "vectorDim": 10
      }
    },
    "inputData": {
      "className": "org.apache.flink.ml.benchmark.data.common.DenseVectorGenerator",
      "paramMap": {
        "seed": null,
        "colNames": ["features"],
        "numValues": 10000,
        "vectorDim": 10
      }
    }
  }
}
```

Looking back into this configuration file, it is now clear that this
configuration file contains one benchmark. The benchmark name is
"KMeansModel-1", and is executed on `KMeansModel` stage. The model data of the
stage is 2 vector-typed centroids with 10 dimensions, and the stage is
benchmarked against 10000 randomly generated vectors.
