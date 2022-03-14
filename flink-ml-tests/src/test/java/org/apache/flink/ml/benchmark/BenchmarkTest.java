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

import org.apache.flink.ml.clustering.kmeans.KMeansBenchmark;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests {@link Benchmark}. */
public class BenchmarkTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    // Tests the whole life cycle of using a benchmark, including loading from file, executing the
    // benchmark, check results, and save results to file.
    @Test
    public void testLifeCycle() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Benchmark<?> benchmark =
                new KMeansBenchmark()
                        .setName("KMeansBenchmark1")
                        .setMaxIter(2)
                        .setDataSize(10000)
                        .setDims(10);

        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-test-conf.json");

        List<Benchmark<?>> actualBenchmarks = Benchmark.parseFrom(inputStream);

        assertEquals(actualBenchmarks.size(), 1);
        assertEquals(benchmark.getName(), actualBenchmarks.get(0).getName());
        assertEquals(benchmark.getParamMap(), actualBenchmarks.get(0).getParamMap());

        long estimatedTime = System.currentTimeMillis();
        BenchmarkResult result = benchmark.execute(tEnv);
        estimatedTime = System.currentTimeMillis() - estimatedTime;

        assertTrue(result.getExecutionTime() > 0);
        assertTrue(result.getExecutionTime() <= estimatedTime);

        String savePath = tempFolder.newFolder().getAbsolutePath();
        String saveFileName = savePath + "/benchmark.txt";

        BenchmarkResult.save(saveFileName, result);

        BufferedReader reader = new BufferedReader(new FileReader(saveFileName));

        String savedContent = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        assertTrue(savedContent.contains(benchmark.getName()));
        assertTrue(savedContent.contains(Long.toString(result.getExecutionTime())));
    }
}
