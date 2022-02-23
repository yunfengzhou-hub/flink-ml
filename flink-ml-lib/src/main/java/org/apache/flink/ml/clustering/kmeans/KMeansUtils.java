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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Random;

/** Utility methods for KMeans algorithm. */
public class KMeansUtils {
    /**
     * Generates a Table containing a {@link KMeansModelData} instance with randomly generated
     * centroids.
     *
     * @param env The environment where to create the table.
     * @param k The number of generated centroids.
     * @param dim The size of generated centroids.
     * @param weight The weight of the centroids.
     * @param seed Random seed.
     */
    public static Table generateRandomModelData(
            StreamExecutionEnvironment env, int k, int dim, double weight, long seed) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        return tEnv.fromDataStream(
                env.fromElements(1).map(new RandomCentroidsCreator(k, dim, weight, seed)));
    }

    private static class RandomCentroidsCreator implements MapFunction<Integer, KMeansModelData> {
        private final int k;
        private final int dim;
        private final long seed;
        private final double weight;

        private RandomCentroidsCreator(int k, int dim, double weight, long seed) {
            this.k = k;
            this.dim = dim;
            this.seed = seed;
            this.weight = weight;
        }

        @Override
        public KMeansModelData map(Integer integer) {
            DenseVector[] centroids = new DenseVector[k];
            Random random = new Random(seed);
            for (int i = 0; i < k; i++) {
                centroids[i] = new DenseVector(dim);
                for (int j = 0; j < dim; j++) {
                    centroids[i].values[j] = random.nextDouble();
                }
            }
            DenseVector weights = new DenseVector(k);
            Arrays.fill(weights.values, weight);
            return new KMeansModelData(centroids, weights);
        }
    }
}
