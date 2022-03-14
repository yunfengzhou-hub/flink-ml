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
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.WithParams;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.ml.util.ReadWriteUtils.OBJECT_MAPPER;

/**
 * A {@link Benchmark} object represents a benchmark instance to be executed.
 *
 * <p>NOTE: every Benchmark subclass should have a no-arg constructor.
 */
public abstract class Benchmark<T extends Benchmark<T>> implements BenchmarkParams<T> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private String name;

    protected Benchmark() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    /**
     * Executes the current benchmark instance on the provided {@link StreamTableEnvironment}, and
     * returns a {@link BenchmarkResult}.
     *
     * @throws Exception if the execution process fails.
     */
    public abstract BenchmarkResult execute(StreamTableEnvironment tEnv) throws Exception;

    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public T setName(String name) {
        this.name = name;
        return (T) this;
    }

    /**
     * Parse a list of benchmark instances from the input stream containing benchmark parameters in
     * json format.
     */
    @SuppressWarnings("unchecked")
    public static List<Benchmark<?>> parseFrom(InputStream inputStream) throws IOException {
        Map<String, ?> jsonMap = OBJECT_MAPPER.readValue(inputStream, Map.class);
        List<Map<String, ?>> benchmarkParams = (List<Map<String, ?>>) jsonMap.get("benchmarks");
        List<Benchmark<?>> benchmarks = new ArrayList<>();

        for (Map<String, ?> metadata : benchmarkParams) {
            String className = (String) metadata.get("className");
            Map<String, String> paramMap = (Map<String, String>) metadata.get("paramMap");
            try {
                Class<Benchmark<?>> clazz = (Class<Benchmark<?>>) Class.forName(className);
                Benchmark<?> instance = InstantiationUtil.instantiate(clazz);
                instance.setName((String) metadata.get("name"));

                Map<String, Param<?>> nameToParam = new HashMap<>();
                for (Param<?> param : ParamUtils.getPublicFinalParamFields(instance)) {
                    nameToParam.put(param.name, param);
                }

                for (Map.Entry<String, String> entry : paramMap.entrySet()) {
                    Param<?> param = nameToParam.get(entry.getKey());
                    setParam(instance, param, param.jsonDecode(entry.getValue()));
                }
                benchmarks.add(instance);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load stage.", e);
            }
        }

        return benchmarks;
    }

    // A helper method that sets benchmark's parameter value. We can not call stage.set(param,
    // value)
    // directly because stage::set(...) needs the actual type of the value.
    @SuppressWarnings("unchecked")
    private static <T> void setParam(WithParams<?> benchmark, Param<T> param, Object value) {
        benchmark.set(param, (T) value);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Benchmark<?>> benchmarks = new ArrayList<>(); // parse from configuration file
        benchmarks.add(new KMeansBenchmark());

        for (Benchmark<?> benchmark : benchmarks) {
            benchmark.execute(tEnv);
        }
    }
}
