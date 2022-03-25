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
import org.apache.flink.ml.common.param.HasGlobalBatchSize;
import org.apache.flink.ml.common.param.HasSeed;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

/**
 * Params of {@link OnlineKMeans}.
 *
 * @param <T> The class type of this instance.
 */
public interface OnlineKMeansParams<T>
        extends HasBatchStrategy<T>,
                HasGlobalBatchSize<T>,
                HasDecayFactor<T>,
                HasSeed<T>,
                KMeansModelParams<T> {
    Param<Integer> DIM =
            new IntParam(
                    "dim",
                    "The dimension of random centroids to create.",
                    2,
                    ParamValidators.gtEq(1));

    Param<Double> INIT_WEIGHT =
            new DoubleParam(
                    "initWeight",
                    "The initial weight of random centroids to create.",
                    0.,
                    ParamValidators.gtEq(0));
}
