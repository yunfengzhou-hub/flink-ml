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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.function.StreamFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.function.Function;

public class PipelineUtils {
    /**
     * create a {@link Function} that represents the computation logic of the provided {@link Pipeline}.
     * This method does the following:
     * - Read Json string from the specified file path
     * - Use the Json string to initialize the pipeline
     * - Convert the computation logic of the pipeline into a Function.
     *
     * @param path_to_json_file path and file name of the json file that stores the json representation of the pipeline
     * @param <IN> Class of the input of the {@link Pipeline}
     * @param <OUT> Class of the output of the {@link Pipeline}
     * @return a {@link Function} with the computation logic
     * @throws Exception
     */
    public static <IN, OUT> Function<IN, List<OUT>> toFunction(String path_to_json_file, Class<IN> inClass, Class<OUT> outClass) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path_to_json_file));
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

    public static <IN, OUT> Function<IN, List<OUT>> toFunction(Pipeline pipeline, Class<IN> inClass, Class<OUT> outClass) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream inStream = env.fromElements(inClass.getDeclaredConstructor().newInstance());
        Table input_table = tEnv.fromDataStream(inStream);

        Table output_table = pipeline.transform(tEnv, input_table);

        DataStream outStream = tEnv.toAppendStream(output_table, outClass);

        return new StreamFunction<>(outStream);
    }


}
