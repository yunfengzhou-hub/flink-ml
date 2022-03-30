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

import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Entry class for benchmark execution. */
public class Benchmark {
    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InputStream inputStream = new FileInputStream(args[0]);
        Map<String, ?> jsonMap = ReadWriteUtils.OBJECT_MAPPER.readValue(inputStream, Map.class);
        Preconditions.checkArgument(
                jsonMap.containsKey("version") && jsonMap.get("version").equals(1));

        List<String> benchmarkNames =
                jsonMap.keySet().stream()
                        .filter(x -> !x.equals("version"))
                        .collect(Collectors.toList());
        LOG.info("Found benchmarks " + benchmarkNames);

        List<BenchmarkResult> results = new ArrayList<>();

        for (String benchmarkName : benchmarkNames) {
            LOG.info("Running benchmark " + benchmarkName + ".");

            BenchmarkResult result =
                    BenchmarkUtils.runBenchmark(
                            env, benchmarkName, (Map<String, ?>) jsonMap.get(benchmarkName));

            results.add(result);
        }

        for (BenchmarkResult result : results) {
            BenchmarkUtils.printResult(result);
        }

        if (args.length > 1) {
            String savePath = args[1];
            BenchmarkUtils.saveResultsAsJson(savePath, results);
            LOG.info("Benchmark results saved as json in " + savePath + ".");
        }
    }
}
