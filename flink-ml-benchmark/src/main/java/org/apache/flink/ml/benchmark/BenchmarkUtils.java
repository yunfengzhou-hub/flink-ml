/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.benchmark;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.benchmark.generator.DataGenerator;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Utility methods for benchmarks. */
public class BenchmarkUtils {
    /**
     * Instantiates a benchmark from its parameter map and executes the benchmark in the provided
     * environment.
     *
     * @return Results of the executed benchmark.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static BenchmarkResult runBenchmark(
            StreamTableEnvironment tEnv, String name, Map<String, ?> params) throws Exception {
        Stage stage = ReadWriteUtils.instantiateWithParams((Map<String, ?>) params.get("stage"));
        DataGenerator inputsGenerator =
                ReadWriteUtils.instantiateWithParams((Map<String, ?>) params.get("inputs"));
        DataGenerator modelDataGenerator = null;
        if (params.containsKey("modelData")) {
            modelDataGenerator =
                    ReadWriteUtils.instantiateWithParams((Map<String, ?>) params.get("modelData"));
        }

        return runBenchmark(tEnv, name, stage, inputsGenerator, modelDataGenerator);
    }

    /**
     * Executes a benchmark from a stage with its inputsGenerator in the provided environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            StreamTableEnvironment tEnv,
            String name,
            Stage<?> stage,
            DataGenerator<?> inputsGenerator)
            throws Exception {
        return runBenchmark(tEnv, name, stage, inputsGenerator, null);
    }

    /**
     * Executes a benchmark from a stage with its inputsGenerator and modelDataGenerator in the
     * provided environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            StreamTableEnvironment tEnv,
            String name,
            Stage<?> stage,
            DataGenerator<?> inputsGenerator,
            DataGenerator<?> modelDataGenerator)
            throws Exception {
        StreamExecutionEnvironment env = TableUtils.getExecutionEnvironment(tEnv);

        Table[] inputTables = inputsGenerator.getData(tEnv);
        if (modelDataGenerator != null) {
            ((Model<?>) stage).setModelData(modelDataGenerator.getData(tEnv));
        }

        Table[] outputTables;
        if (stage instanceof Estimator) {
            outputTables = ((Estimator<?, ?>) stage).fit(inputTables).getModelData();
        } else if (stage instanceof AlgoOperator) {
            outputTables = ((AlgoOperator<?>) stage).transform(inputTables);
        } else {
            throw new IllegalArgumentException("Unsupported Stage class " + stage.getClass());
        }

        for (Table table : outputTables) {
            tEnv.toDataStream(table).addSink(new CountingAndDiscardingSink<>());
        }

        JobExecutionResult executionResult = env.execute();

        BenchmarkResult result = new BenchmarkResult();
        result.name = name;
        result.totalTimeMs = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        result.inputRecordNum = inputsGenerator.getNumData();
        result.inputThroughput = result.inputRecordNum * 1000.0 / result.totalTimeMs;
        result.outputRecordNum =
                executionResult.getAccumulatorResult(CountingAndDiscardingSink.COUNTER_NAME);
        result.outputThroughput = result.outputRecordNum * 1000.0 / result.totalTimeMs;

        return result;
    }

    /** Prints out the provided benchmark result. */
    public static void printResult(BenchmarkResult result) {
        Preconditions.checkNotNull(result.name);
        System.out.println("Benchmark Name: " + result.name);
        System.out.println("Total Execution Time: " + result.totalTimeMs + " ms");
        System.out.println("Total Input Record Number: " + result.inputRecordNum);
        System.out.println(
                "Average Input Throughput: " + result.inputThroughput + " events per second");
        System.out.println("Total Output Record Number: " + result.outputRecordNum);
        System.out.println(
                "Average Output Throughput: " + result.outputThroughput + " events per second");
        System.out.println();
    }

    /** Saves the benchmark results to the given file in json format. */
    public static void saveResultsAsJson(String path, List<BenchmarkResult> results)
            throws IOException {
        results.forEach(x -> Preconditions.checkNotNull(x.name));
        List<Map<String, ?>> resultsMap =
                results.stream().map(BenchmarkResult::toMap).collect(Collectors.toList());
        ReadWriteUtils.saveToFile(
                new Path(path),
                ReadWriteUtils.OBJECT_MAPPER
                        .writerWithDefaultPrettyPrinter()
                        .writeValueAsString(resultsMap));
    }
}
