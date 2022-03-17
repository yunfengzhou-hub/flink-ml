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
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.benchmark.generator.DataGenerator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.WithParams;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Utility methods for benchmarks. */
@SuppressWarnings("unchecked")
public class BenchmarkUtils {
    private static final String benchmarkNamePattern = "^[A-Za-z0-9][A-Za-z0-9_\\-]*$";

    /**
     * Loads benchmark paramMaps from the provided json map.
     *
     * @return A map whose key is the names of the loaded benchmarks, value is the parameters of the
     *     benchmarks.
     */
    public static Map<String, Map<String, ?>> parseBenchmarkParams(Map<String, ?> jsonMap) {
        Map<String, Map<String, ?>> result = new HashMap<>();
        for (String key : jsonMap.keySet()) {
            if (!isValidBenchmarkName(key)) {
                continue;
            }
            result.put(key, (Map<String, ?>) jsonMap.get(key));
        }
        return result;
    }

    /**
     * Checks whether a string is a valid benchmark name.
     *
     * <p>A valid benchmark name should only contain English letters, numbers, hyphens (-) and
     * underscores (_). The name should not start with a hyphen or underscore.
     */
    public static boolean isValidBenchmarkName(String name) {
        return Pattern.matches(benchmarkNamePattern, name);
    }

    /**
     * Instantiates a benchmark from its parameter map and executes the benchmark in the provided
     * environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            String name, StreamTableEnvironment tEnv, Map<String, ?> jsonMap) throws Exception {
        Stage<?> stage = (Stage<?>) parseInstance((Map<String, ?>) jsonMap.get("stage"));

        BenchmarkResult result;
        if (jsonMap.size() == 2 && jsonMap.containsKey("inputs")) {
            DataGenerator<?> inputsGenerator =
                    (DataGenerator<?>) parseInstance((Map<String, ?>) jsonMap.get("inputs"));
            result = runBenchmark(name, tEnv, stage, inputsGenerator);
        } else if (jsonMap.size() == 3 && stage instanceof Model) {
            DataGenerator<?> inputsGenerator =
                    (DataGenerator<?>) parseInstance((Map<String, ?>) jsonMap.get("inputs"));
            DataGenerator<?> modelDataGenerator =
                    (DataGenerator<?>) parseInstance((Map<String, ?>) jsonMap.get("modelData"));
            result =
                    runBenchmark(name, tEnv, (Model<?>) stage, modelDataGenerator, inputsGenerator);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported json map with keys " + jsonMap.keySet());
        }

        return result;
    }

    /**
     * Executes a benchmark from a stage and an inputsGenerator in the provided environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            String name,
            StreamTableEnvironment tEnv,
            Stage<?> stage,
            DataGenerator<?> inputsGenerator)
            throws Exception {
        Table[] inputTables = inputsGenerator.getData(tEnv);

        Table[] outputTables;
        if (stage instanceof Estimator) {
            outputTables = ((Estimator<?, ?>) stage).fit(inputTables).getModelData();
        } else if (stage instanceof AlgoOperator) {
            outputTables = ((AlgoOperator<?>) stage).transform(inputTables);
        } else {
            throw new IllegalArgumentException("Unsupported Stage class " + stage.getClass());
        }

        for (Table table : outputTables) {
            tEnv.toDataStream(table).addSink(new DiscardingSink<>());
        }

        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        JobExecutionResult executionResult = env.execute();

        BenchmarkResult result = new BenchmarkResult();
        result.name = name;
        result.executionTimeMillis = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);

        return result;
    }

    /**
     * Executes a benchmark from a model with its modelDataGenerator and inputsGenerator in the
     * provided environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            String name,
            StreamTableEnvironment tEnv,
            Model<?> model,
            DataGenerator<?> modelDataGenerator,
            DataGenerator<?> inputsGenerator)
            throws Exception {
        model.setModelData(modelDataGenerator.getData(tEnv));

        Table[] outputTables = model.transform(inputsGenerator.getData(tEnv));

        for (Table table : outputTables) {
            tEnv.toDataStream(table).addSink(new DiscardingSink<>());
        }

        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        JobExecutionResult executionResult = env.execute();

        BenchmarkResult result = new BenchmarkResult();
        result.name = name;
        result.executionTimeMillis = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);

        return result;
    }

    /**
     * Instantiates a WithParams subclass from the provided json map.
     *
     * @param jsonMap a map containing className and paramMap.
     * @return the instantiated WithParams subclass.
     */
    public static WithParams<?> parseInstance(Map<String, ?> jsonMap) throws Exception {
        String className = (String) jsonMap.get("className");
        Class<WithParams<?>> clazz = (Class<WithParams<?>>) Class.forName(className);
        WithParams<?> instance = InstantiationUtil.instantiate(clazz);

        Map<String, Param<?>> nameToParam = new HashMap<>();
        for (Param<?> param : ParamUtils.getPublicFinalParamFields(instance)) {
            nameToParam.put(param.name, param);
        }

        Map<String, String> paramMap = (Map<String, String>) jsonMap.get("paramMap");
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            Param<?> param = nameToParam.get(entry.getKey());
            setParam(instance, param, param.jsonDecode(entry.getValue()));
        }

        return instance;
    }

    // A helper method that sets benchmark's parameter value. We can not call stage.set(param,
    // value)
    // directly because stage::set(...) needs the actual type of the value.
    private static <T> void setParam(WithParams<?> benchmark, Param<T> param, Object value) {
        benchmark.set(param, (T) value);
    }

    /** Prints out the provided benchmark result. */
    public static void printResult(BenchmarkResult result) {
        Preconditions.checkNotNull(result.name);
        System.out.println("Benchmark Name: " + result.name);

        if (result.executionTimeMillis != null) {
            System.out.println("Total Execution Time(ms): " + result.executionTimeMillis);
        }

        if (result.throughputTPS != null) {
            System.out.println("Average Throughput(tps): " + result.throughputTPS);
        }

        if (result.latencyMillis != null) {
            System.out.println("Average latency(ms): " + result.latencyMillis);
        }

        System.out.println();
    }
}
