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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.common.param.HasDecayFactor;
import org.apache.flink.ml.common.param.HasSeed;
import org.apache.flink.ml.param.*;

/**
 * Params of {@link StreamingKMeans}.
 *
 * @param <T> The class type of this instance.
 */
public interface StreamingKMeansParams<T>
        extends HasBatchStrategy<T>, HasDecayFactor<T>, HasSeed<T>, KMeansModelParams<T> {
    Param<String> INIT_MODE =
            new StringParam(
                    "initMode",
                    "How to initialize the model data of the online KMeans algorithm. Supported options: 'random', 'direct'.",
                    "random",
                    ParamValidators.inArray("random", "direct"));

    Param<Integer> DIMS =
            new IntParam(
                    "dims",
                    "The number of dimensions of centroids. Used when initializing random centroids.",
                    1,
                    ParamValidators.gt(0));

    Param<Double[]> INIT_WEIGHTS =
            new DoubleArrayParam(
                    "initWeights",
                    "The weight of the initial centroids.",
                    null,
                    ParamValidators.nonEmptyArray());

    default String getInitMode() {
        return get(INIT_MODE);
    }

    default T setInitMode(String value) {
        return set(INIT_MODE, value);
    }

    default int getDims() {
        return get(DIMS);
    }

    default T setDims(int value) {
        return set(DIMS, value);
    }

    default Double[] getInitWeights() {
        return get(INIT_WEIGHTS);
    }

    default T setInitWeights(Double[] value) {
        return set(INIT_WEIGHTS, value);
    }
}
