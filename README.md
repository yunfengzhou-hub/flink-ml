This branch provides example codes to illustrate the performance of
flink-ml-iteration.

In order to run a performance benchmark of flink-ml-iteration, follow the
instructions below.

1. prerequisites
   1. you have installed flink 1.15.0 in your environment and configured
      $FLINK_HOME.
   2. you have installed maven in your environment.
2. under the root directory of this repository, execute the following commands
   in order
```shell
mvn clean install -DskipTests
cd ./flink-ml-dist/target/flink-ml-*-bin/flink-ml*/
cp ./lib/*.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/start-cluster.sh
$FLINK_HOME/bin/flink run -c org.apache.flink.ml.benchmark.IterationBenchmark $FLINK_HOME/lib/flink-ml-uber*.jar --iteration=10 --repeat=10
```

The last command above would submit a flink job which contains an iteration body
and light stream operators whose computation overhead can be ignored, measures
its execution time, and repeats this operation several times to eliminate the
influence of the warmup process. The parameters are as follows.

- `iteration`: number of iterations to be executed in the flink job.
- `repeat`: number of times to repeat submitting and executing the flink job.
- 