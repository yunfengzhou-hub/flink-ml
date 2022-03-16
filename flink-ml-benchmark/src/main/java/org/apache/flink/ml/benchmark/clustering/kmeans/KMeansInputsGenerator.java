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

package org.apache.flink.ml.benchmark.clustering.kmeans;

import org.apache.flink.ml.benchmark.generator.DataGenerator;
import org.apache.flink.ml.benchmark.generator.GeneratorParams;
import org.apache.flink.ml.benchmark.generator.GeneratorUtils;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that generates table arrays containing inputs for {@link
 * org.apache.flink.ml.clustering.kmeans.KMeans} operator.
 */
public class KMeansInputsGenerator
        implements DataGenerator<KMeansInputsGenerator>,
                GeneratorParams<KMeansInputsGenerator>,
                KMeansParams<KMeansInputsGenerator> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeansInputsGenerator() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] getData(StreamTableEnvironment tEnv) {
        Table dataTable =
                GeneratorUtils.generateRandomContinuousVectorStream(
                        tEnv, getDataSize(), getSeed(), getDims());
        return new Table[] {dataTable.as(getFeaturesCol())};
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
