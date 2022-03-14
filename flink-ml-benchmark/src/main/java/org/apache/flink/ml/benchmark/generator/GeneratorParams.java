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

package org.apache.flink.ml.benchmark.generator;

import org.apache.flink.ml.common.param.HasSeed;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.LongParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

/** Interface for the generator params. */
public interface GeneratorParams<T> extends HasSeed<T> {
    Param<Long> NUM_DATA =
            new LongParam("numData", "Number of data to be generated.", 10L, ParamValidators.gt(0));

    Param<Integer> DIMS =
            new IntParam(
                    "dims",
                    "Dimension of vector-typed data to be generated.",
                    1,
                    ParamValidators.gt(0));

    default long getNumData() {
        return get(NUM_DATA);
    }

    default T setNumData(long value) {
        return set(NUM_DATA, value);
    }

    default int getDims() {
        return get(DIMS);
    }

    default T setDims(int value) {
        return set(DIMS, value);
    }
}
