# Flink ML Benchmark Guideline

This document provides instructions about how to run benchmarks on Flink ML's
stages.

## Write Benchmark Programs

### Add Maven Dependencies

In order to write Flink ML's java benchmark programs, first make sure that the
following dependencies have been added to your maven project's `pom.xml`.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml-core_${scala.binary.version}</artifactId>
  <version>${flink.ml.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml-iteration_${scala.binary.version}</artifactId>
  <version>${flink.ml.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml-lib_${scala.binary.version}</artifactId>
  <version>${flink.ml.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml-benchmark_${scala.binary.version}</artifactId>
  <version>${flink.ml.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>statefun-flink-core</artifactId>
  <version>3.1.0</version>
  <exclusions>
    <exclusion>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java_2.12</artifactId>
    </exclusion>
  </exclusions>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
  <version>${flink.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
  <version>${flink.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
  <version>${flink.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients_${scala.binary.version}</artifactId>
  <version>${flink.version}</version>
</dependency>
```

### Write Java Program

Then you can write a program as follows to run benchmark on Flink ML stages. The
example code below tests the performance of Flink ML's KMeans algorithm, with
the default configuration parameters used.

```java
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KMeans kMeans = new KMeans();
        KMeansInputsGenerator inputsGenerator = new KMeansInputsGenerator();

        BenchmarkResult result =
                BenchmarkUtils.runBenchmark("exampleBenchmark", tEnv, kMeans, inputsGenerator);

        BenchmarkUtils.printResult(result);
    }
}
```

### Execute Benchmark Program

After executing the `main()` method above, you will see benchmark results
printed out in your terminal. An example of the printed content is as follows.

```
Benchmark Name: exampleBenchmark
Total Execution Time(ms): 828.0
```

### Configure Benchmark Parameters

If you want to run benchmark on customed configuration parameters, you can set
them with Flink ML's `WithParams` API as follows.

```java
KMeans kMeans = new KMeans()
  .setK(5)
  .setMaxIter(50);
KMeansInputsGenerator inputsGenerator = new KMeansInputsGenerator()
  .setDims(3)
  .setDataSize(10000);
```

## Execute Benchmark through Command-Line Interface (CLI)

You can also configure and execute benchmarks through Command-Line Interface
(CLI) without writing java programs.

### Prerequisites

Before using Flink ML's CLI, make sure you have installed Flink 1.14 in your
local environment, and that you have started a Flink cluster locally. If not,
you can start a standalone session with the following command.

```bash
$ start-cluster
```

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

### Run Benchmark CLI

In the binary distribution's folder, execute the following command to run an
example benchmark.

```bash
$ ./bin/flink-ml-benchmark.sh ./examples/benchmark-example-conf.json
```

You will notice that some Flink job is submitted to your Flink cluster, and the
following information is printed out in your terminal. This means that you have
successfully executed a benchmark on `KMeansModel`.

```
Job has been submitted with JobID 85b4a33df5c00a315e0d1142e1d743be
Program execution finished
Job with JobID 85b4a33df5c00a315e0d1142e1d743be has finished.
Job Runtime: 828 ms

Benchmark Name: KMeansModel-1
Total Execution Time(ms): 828.0

```

### Save Benchmark Result to File

`flink-ml-benchmark.sh` has redirected all warnings and process logs to stderr,
and the benchmark results to stdout. So if you write the command in the
following way

```bash
$ ./bin/flink-ml-benchmark.sh ./examples/benchmark-example-conf.json > output.txt
```

You will get a clean benchmark result saved in `output.txt` as follows.

```
Benchmark Name: KMeansModel-1
Total Execution Time(ms): 828.0

```

### Configuration File Format

The benchmark CLI creates stages and test data according to the input
configuration file. The configuration file should contain a JSON object in the
following format.

First of all, the file should contain the following configurations as the
metadata of the JSON object.

- `"_version"`: The version of the json format. Currently its value must be 1.

Then, each benchmark should be specified through key-object pairs.

The key for each benchmark JSON object will be regarded as the name of the
benchmark. The benchmark name can contain English letters, numbers, hyphens(-)
and underscores(_), but should not start with a hyphen or underscore.

The value of each benchmark name should be a JSON object containing at least
`"stage"` and `"inputs"`. If the stage to be tested is a `Model` and its model
data needs to be explicitly set, the JSON object should also contain
`"modelData"`.

The value of `"stage"`, `"inputs"` or `"modelData"` should be a JSON object
containing `"className"` and `paramMap`.

- `"className"`'s value should be the full classpath of a `WithParams` subclass.
  For `"stage"`, the class should be a subclass of `Stage`. For `"inputs"` or
  `"modelData"`, the class should be a subclass of `DataGenerator`.

- `"paramMap"`'s value should be a JSON object containing the parameters related
  to the specific `Stage` or `DataGenerator`. Note that all parameter values
  should be wrapped as strings.

Combining the format requirements above, an example configuration file is as
follows.

```json
{
  "_version": 1,
  "KMeansModel-1": {
    "stage": {
      "className": "org.apache.flink.ml.clustering.kmeans.KMeansModel",
      "paramMap": {
        "featuresCol": "\"features\"",
        "k": "2",
        "distanceMeasure": "\"euclidean\"",
        "predictionCol": "\"prediction\""
      }
    },
    "modelData":  {
      "className": "org.apache.flink.ml.benchmark.clustering.kmeans.KMeansModelDataGenerator",
      "paramMap": {
        "seed": "null",
        "k": "2",
        "dims": "10"
      }
    },
    "inputs": {
      "className": "org.apache.flink.ml.benchmark.clustering.kmeans.KMeansInputsGenerator",
      "paramMap": {
        "seed": "null",
        "featuresCol": "\"features\"",
        "numData": "10000",
        "dims": "10"
      }
    }
  }
}

```
