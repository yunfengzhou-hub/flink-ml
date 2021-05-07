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

package org.apache.flink.ml.common.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.function.EmbedStreamFunction;
import org.apache.flink.ml.common.function.StreamFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;

@PublicEvolving
public class PipelineUtils {
    /**
     * create a {@link StreamFunction} that represents the computation logic of the provided {@link Pipeline}.
     *
     * <p> This method does the following:
     *
     * <ul>
     *   <li> Read Json string from the specified file path
     *   <li> Use the Json string to initialize the {@link Pipeline}
     *   <li> Convert the computation logic of the pipeline into a {@link StreamFunction}.
     * </ul>
     *
     * <p> In order for a {@link Pipeline} to be able to be converted to {@link StreamFunction}, the {@link Pipeline}
     * needs to obey the following requirements.
     *
     * <ul>
     *   <li> Only stateless streaming operators are used in the {@link Pipeline}.
     *   <li> Operators do not rely on any external {@link org.apache.flink.table.api.TableEnvironment} object, which
     *        means operators do not read or write table to the environment, nor do they invoke functions registered
     *        in the environment.
     *   <li> The {@link Pipeline} does not contain {@link org.apache.flink.ml.api.core.Estimator}s, only
     *        {@link org.apache.flink.ml.api.core.Transformer}s.
     * </ul>
     *
     * <p> If unsupported operators are used or any other requirements above are violated, function will not be
     * generated and exceptions will be thrown.
     *
     * @param pipelineJsonFilePath path and file name of the file that stores the json representation of the pipeline
     * @param inClass class of input objects. Must be POJO.
     * @param outClass class of output objects. Must be POJO.
     * @param <IN> Class of the input of the {@link Pipeline}
     * @param <OUT> Class of the output of the {@link Pipeline}
     * @return a {@link StreamFunction} with the computation logic
     * @throws Exception if the provided path is invalid or a function cannot be produced.
     */
    public static <IN, OUT> StreamFunction<IN, OUT> toFunction(
            String pipelineJsonFilePath, Class<IN> inClass, Class<OUT> outClass) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(pipelineJsonFilePath));
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();

        while (line != null) {
            sb.append(line);
            sb.append(System.lineSeparator());
            line = br.readLine();
        }

        String pipelineJson = sb.toString();

        return toFunction(new Pipeline(pipelineJson), inClass, outClass);
    }

    /**
     * Similar to {@link #toFunction(String, Class, Class)}, except that users can directly pass a {@link Pipeline} object.
     */
    public static <IN, OUT> StreamFunction<IN, OUT> toFunction(Pipeline pipeline, Class<IN> inClass, Class<OUT> outClass) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<IN> inStream = env.fromElements(inClass.getDeclaredConstructor().newInstance());
        Table input_table = tEnv.fromDataStream(inStream);

        Table output_table = pipeline.transform(tEnv, input_table);

        DataStream<OUT> outStream = tEnv.toAppendStream(output_table, outClass);

        return new EmbedStreamFunction<>(outStream);
    }
}
