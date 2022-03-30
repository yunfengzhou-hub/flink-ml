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
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.ml.util.ReadWriteUtils.OBJECT_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests benchmarks. */
@SuppressWarnings("unchecked")
public class BenchmarkTest extends AbstractTestBase {
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
    public void testCreateAndExecute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KMeans kMeans = new KMeans().setK(5).setFeaturesCol("test_feature");
        KMeansInputsGenerator inputsGenerator =
                new KMeansInputsGenerator()
                        .setK(5)
                        .setFeaturesCol("test_feature")
                        .setNumData(1000)
                        .setDims(10);

        BenchmarkResult result =
                BenchmarkUtils.runBenchmark(env, "testBenchmarkName", kMeans, inputsGenerator);

        BenchmarkUtils.printResult(result);
        assertTrue(outContent.toString().contains("testBenchmarkName"));
        assertTrue(outContent.toString().contains(result.totalTimeMs.toString()));
    }

    @Test
    public void testLoadAndSave() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-example-conf.json");
        Map<String, ?> jsonMap = OBJECT_MAPPER.readValue(inputStream, Map.class);
        List<String> benchmarkNames =
                jsonMap.keySet().stream()
                        .filter(x -> !x.equals("version"))
                        .collect(Collectors.toList());

        assertEquals(1, benchmarkNames.size());
        assertTrue(benchmarkNames.contains("KMeansModel-1"));

        KMeansModel expectedStage = new KMeansModel();
        WithParams<?> actualStage =
                ReadWriteUtils.instantiateWithParams(
                        (Map<String, ?>)
                                ((Map<String, ?>) jsonMap.get(benchmarkNames.get(0))).get("stage"));
        assertEquals(expectedStage.getClass(), actualStage.getClass());
        assertEquals(expectedStage.getParamMap(), actualStage.getParamMap());

        List<String> expectedContents = new ArrayList<>();
        List<BenchmarkResult> results = new ArrayList<>();

        for (String benchmarkName : benchmarkNames) {
            long estimatedTime = System.currentTimeMillis();
            BenchmarkResult result =
                    BenchmarkUtils.runBenchmark(
                            env, benchmarkName, (Map<String, ?>) jsonMap.get(benchmarkName));
            estimatedTime = System.currentTimeMillis() - estimatedTime;

            assertTrue(result.totalTimeMs > 0);
            assertTrue(result.totalTimeMs <= estimatedTime);

            results.add(result);
            expectedContents.add(benchmarkName);
            expectedContents.add(result.totalTimeMs.toString());
        }

        Path savePath = Paths.get(tempFolder.newFolder().getAbsolutePath(), "result.json");

        BenchmarkUtils.saveResultsAsJson(savePath.toString(), results);
        String actualContent = new String(Files.readAllBytes(savePath));

        for (String expectedContent : expectedContents) {
            assertTrue(actualContent.contains(expectedContent));
        }
    }

    @Test
    public void testMain() throws Exception {
        File configFile = new File(tempFolder.newFolder().getAbsolutePath() + "/test-conf.json");
        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-example-conf.json");
        FileUtils.copyInputStreamToFile(inputStream, configFile);

        Path savePath = Paths.get(tempFolder.newFolder().getAbsolutePath(), "conf.json");

        Benchmark.main(new String[] {configFile.getAbsolutePath(), savePath.toString()});

        String actualContent = new String(Files.readAllBytes(savePath));

        assertTrue(outContent.toString().contains("KMeansModel-1"));
        assertTrue(actualContent.contains("KMeansModel-1"));
        // Checks saved content is valid JSON.
        OBJECT_MAPPER.readValue(actualContent, List.class);
    }
}
