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

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.WithParams;

/** Interface for the shared benchmark params. */
public interface BenchmarkParams<T> extends WithParams<T> {
    Param<Integer> DATA_SIZE =
            new IntParam(
                    "dataSize", "Size of test data to be generated.", 20, ParamValidators.gt(0));

    Param<Integer> DIMS =
            new IntParam("dims", "Size of data to be generated.", 20, ParamValidators.gt(0));

    default int getDataSize() {
        return get(DATA_SIZE);
    }

    default T setDataSize(int value) {
        return set(DATA_SIZE, value);
    }

    default int getDims() {
        return get(DIMS);
    }

    default T setDims(int value) {
        return set(DIMS, value);
    }
}
