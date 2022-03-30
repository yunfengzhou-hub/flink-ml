# Flink ML Benchmark Getting Started

This document provides instructions about how to run benchmarks on Flink ML's
stages in a Linux/MacOS environment.

## Prerequisites

### Installing Flink

Please make sure Flink 1.14 or higher version has been installed in your local
environment. You can refer to the [local
installation](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/local_installation/)
instruction on Flink's document website for how to achieve this.

### Setting Up Flink Environment Variables

After having installed Flink, please register `$FLINK_HOME` as an environment
variable into your local environment, and add `$FLINK_HOME` into your `$PATH`
variable. This can be completed by running the following commands in the Flink's
folder.

```bash
export FLINK_HOME=`pwd`
export PATH=$FLINK_HOME/bin:$PATH
```

Then please run the following command. If this command returns 1.14.0 or a
higher version, then it means that the required Flink environment has been
successfully installed and registered in your local environment.

```bash
flink --version
```

### Acquiring Flink ML Binary Distribution

In order to use Flink ML's CLI you need to have the latest binary distribution
of Flink ML. You can acquire the distribution by building Flink ML's source code
locally, which means to execute the following command in Flink ML repository's
root directory.

```bash
$ mvn clean package -DskipTests
```

After executing the command above, you will be able to find the binary
distribution under
`./flink-ml-dist/target/flink-ml-<version>-bin/flink-ml-<version>/`.

### Starting Flink Cluster

Please start a Flink standalone session in your local environment with the
following command.

```bash
start-cluster.sh
```

You should be able to navigate to the web UI at
[localhost:8081](http://localhost:8081/) to view the Flink dashboard and see
that the cluster is up and running.

## Run Benchmark Example

In Flink ML's binary distribution's folder, execute the following command to run
an example benchmark.

```bash
$ ./bin/flink-ml-benchmark.sh ./examples/benchmark-example-conf.json ./output/results.json
```

You will notice that some Flink job is submitted to your Flink cluster, and the
following information is printed out in your terminal. This means that you have
successfully executed a benchmark on `KMeansModel`.

```
Creating fat jar containing all flink ml dependencies to be submitted.
Job has been submitted with JobID bdaa54b065adf2c813619113a00337de
Program execution finished
Job with JobID bdaa54b065adf2c813619113a00337de has finished.
Job Runtime: 215 ms
Accumulator Results: 
- numElements (java.lang.Long): 10000


Benchmark Name: KMeansModel-1
Total Execution Time: 215.0 ms
Total Input Record Number: 10000
Average Input Throughput: 46511.62790697674 events per second
Total Output Record Number: 10000
Average Output Throughput: 46511.62790697674 events per second

```

The command above would save the results into `./output/results.json` as below.

```json
[ {
  "name" : "KMeansModel-1",
  "totalTimeMs" : 215.0,
  "inputRecordNum" : 10000,
  "inputThroughput" : 46511.62790697674,
  "outputRecordNum" : 10000,
  "outputThroughput" : 46511.62790697674
} ]
```

## Custom Benchmark Configuration File

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

Combining the format requirements above, an example configuration file is as
follows.

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
      "className": "org.apache.flink.ml.benchmark.clustering.kmeans.KMeansModelDataGenerator",
      "paramMap": {
        "seed": null,
        "k": 2,
        "dims": 10
      }
    },
    "inputs": {
      "className": "org.apache.flink.ml.benchmark.clustering.kmeans.KMeansInputsGenerator",
      "paramMap": {
        "seed": null,
        "featuresCol": "features",
        "numData": 10000,
        "dims": 10
      }
    }
  }
}

```
