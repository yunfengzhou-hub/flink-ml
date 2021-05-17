/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.function;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.function.Function;

/**
 * A functional representation of the stream graph of a flink job. The
 * computation logic of the flink job can be invoked through this function's
 * {@link #apply(Object)} method.
 *
 * @param <T> class of the input data
 * @param <R> class of the output data
 */
@PublicEvolving
public interface StreamFunction<T, R> extends Function<T, List<R>> {
    /**
     * executes the computation logic of a stream graph onto the provided input data.
     *
     * @param t input data to be computed
     * @return a list of output data
     */
    List<R> apply(T t);
}
