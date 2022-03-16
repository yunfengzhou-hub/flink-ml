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

import org.apache.flink.ml.benchmark.clustering.kmeans.KMeansInputsGenerator;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.param.WithParams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;

import static org.apache.flink.ml.util.ReadWriteUtils.OBJECT_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests benchmarks. */
@SuppressWarnings("unchecked")
public class BenchmarkTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @Before
    public void before() {
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void after() {
        System.setOut(originalOut);
        outContent.reset();
    }

    @Test
    public void testBenchmarkName() {
        assertTrue(BenchmarkUtils.isValidBenchmarkName("Aa0_-1"));
        assertFalse(BenchmarkUtils.isValidBenchmarkName("_A-1"));
        assertFalse(BenchmarkUtils.isValidBenchmarkName("-A-1"));
    }

    @Test
    public void testExecuteBenchmark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KMeans kMeans = new KMeans();
        KMeansInputsGenerator inputsGenerator = new KMeansInputsGenerator();

        BenchmarkResult result =
                BenchmarkUtils.runBenchmark("testBenchmarkName", tEnv, kMeans, inputsGenerator);

        BenchmarkUtils.printResult(result);
        assertTrue(outContent.toString().contains("testBenchmarkName"));
        assertTrue(outContent.toString().contains(result.executionTimeMillis.toString()));
    }

    @Test
    public void testLoadAndPrint() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-test-conf.json");
        Map<String, ?> jsonMap = OBJECT_MAPPER.readValue(inputStream, Map.class);
        Map<String, Map<String, ?>> benchmarkParamsMap =
                BenchmarkUtils.parseBenchmarkParams(jsonMap);

        assertEquals(1, benchmarkParamsMap.size());
        assertTrue(benchmarkParamsMap.containsKey("KMeansModel-1"));

        KMeansModel expectedStage = new KMeansModel();
        WithParams<?> actualStage =
                BenchmarkUtils.parseInstance(
                        (Map<String, ?>) benchmarkParamsMap.get("KMeansModel-1").get("stage"));
        assertEquals(expectedStage.getClass(), actualStage.getClass());
        assertEquals(expectedStage.getParamMap(), actualStage.getParamMap());

        for (String benchmarkName : benchmarkParamsMap.keySet()) {
            long estimatedTime = System.currentTimeMillis();
            BenchmarkResult result =
                    BenchmarkUtils.runBenchmark(
                            benchmarkName, tEnv, (Map<String, ?>) jsonMap.get(benchmarkName));
            estimatedTime = System.currentTimeMillis() - estimatedTime;

            assertTrue(result.executionTimeMillis > 0);
            assertTrue(result.executionTimeMillis <= estimatedTime);

            BenchmarkUtils.printResult(result);
            assertTrue(outContent.toString().contains(benchmarkName));
            assertTrue(outContent.toString().contains(result.executionTimeMillis.toString()));
        }
    }

    @Test
    public void testMain() throws Exception {
        File configFile = new File(tempFolder.newFolder().getAbsolutePath() + "/test-conf.json");
        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-test-conf.json");
        FileUtils.copyInputStreamToFile(inputStream, configFile);

        Benchmark.main(new String[] {configFile.getAbsolutePath()});

        assertTrue(outContent.toString().contains("KMeansModel-1"));
    }
}