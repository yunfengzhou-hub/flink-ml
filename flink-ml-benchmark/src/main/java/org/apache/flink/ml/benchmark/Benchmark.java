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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.ml.util.ReadWriteUtils.OBJECT_MAPPER;

/** Entry class for benchmark execution. */
public class Benchmark {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        final PrintStream originalOut = System.out;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        System.err.println("Benchmark arguments: " + Arrays.toString(args));

        InputStream inputStream = new FileInputStream(args[0]);
        Map<String, ?> jsonMap = OBJECT_MAPPER.readValue(inputStream, Map.class);
        Map<String, Map<String, ?>> benchmarkParamsMap =
                BenchmarkUtils.parseBenchmarkParams(jsonMap);

        System.err.println("Read paramMap with keys " + benchmarkParamsMap.keySet());

        for (String benchmarkName : benchmarkParamsMap.keySet()) {
            System.err.println("Running benchmark " + benchmarkName);
            System.setOut(System.err);
            BenchmarkResult result =
                    BenchmarkUtils.runBenchmark(
                            benchmarkName, tEnv, (Map<String, ?>) jsonMap.get(benchmarkName));
            System.setOut(originalOut);
            BenchmarkUtils.printResult(result);
        }
    }
}