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
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Utility methods for benchmarks. */
@SuppressWarnings("unchecked")
public class BenchmarkUtils {
    /**
     * Loads benchmark paramMaps from the provided json map.
     *
     * @return A map whose key is the names of the loaded benchmarks, value is the parameters of the
     *     benchmarks.
     */
    public static Map<String, Map<String, ?>> parseBenchmarkParams(Map<String, ?> jsonMap) {
        Preconditions.checkArgument(
                jsonMap.containsKey("version") && jsonMap.get("version").equals(1));

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
        return !name.equals("version");
    }

    /**
     * Instantiates a benchmark from its parameter map and executes the benchmark in the provided
     * environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            StreamExecutionEnvironment env, String name, Map<String, ?> params)
            throws Exception {
        Stage<?> stage = (Stage<?>) instantiateWithParams((Map<String, ?>) params.get("stage"));
        DataGenerator<?> inputsGenerator = (DataGenerator<?>) instantiateWithParams((Map<String, ?>) params.get("inputs"));
        DataGenerator<?> modelDataGenerator = null;
        if (params.containsKey("modelData")) {
            modelDataGenerator = (DataGenerator<?>) instantiateWithParams((Map<String, ?>) params.get("modelData"));
        }

        return runBenchmark(env, name, stage, inputsGenerator, modelDataGenerator);
    }

    public static BenchmarkResult runBenchmark(
            StreamExecutionEnvironment env,
            String name,
            Stage<?> stage,
            DataGenerator<?> inputsGenerator)
            throws Exception {
        return runBenchmark(env, name, stage, inputsGenerator, null);
    }

    /**
     * Executes a benchmark from a stage with its modelDataGenerator and inputsGenerator in the
     * provided environment.
     *
     * @return Results of the executed benchmark.
     */
    public static BenchmarkResult runBenchmark(
            StreamExecutionEnvironment env,
            String name,
            Stage<?> stage,
            DataGenerator<?> inputsGenerator,
            DataGenerator<?> modelDataGenerator)
            throws Exception {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
            tEnv.toDataStream(table).addSink(new DiscardingSink<>());
        }

        JobExecutionResult executionResult = env.execute();

        BenchmarkResult result = new BenchmarkResult();
        result.name = name;
        result.totalTimeMs = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        result.inputRecordNum = inputsGenerator.getNumElements();
        result.inputThroughput = result.inputRecordNum * 1000.0 / result.totalTimeMs;

        return result;
    }

    /**
     * Instantiates a WithParams subclass from the provided json map.
     *
     * @param jsonMap a map containing className and paramMap.
     * @return the instantiated WithParams subclass.
     */
    public static <T extends WithParams<T>> T instantiateWithParams(Map<String, ?> jsonMap) throws Exception {
        String className = (String) jsonMap.get("className");
        Class<T> clazz = (Class<T>) Class.forName(className);
        T instance = InstantiationUtil.instantiate(clazz);

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

    // A helper method that sets an object's parameter value. We can not call stage.set(param,
    // value) directly because stage::set(...) needs the actual type of the value.
    private static <T> void setParam(WithParams<?> withParams, Param<T> param, Object value) {
        withParams.set(param, (T) value);
    }

    /** Prints out the provided benchmark result. */
    public static void printResult(BenchmarkResult result) {
        Preconditions.checkNotNull(result.name);
        System.out.println("Benchmark Name: " + result.name);
        System.out.println("Total Execution Time(ms): " + result.totalTimeMs);
        System.out.println("Total Input Record Number: " + result.inputRecordNum);
        System.out.println("Average Input Throughput(tps): " + result.inputThroughput);
        System.out.println("Total Output Record Number: " + result.outputRecordNum);
        System.out.println("Average Output Throughput(tps): " + result.outputThroughput);
        System.out.println("Average latency(ms): " + result.latencyMs);
        System.out.println();
    }
}
