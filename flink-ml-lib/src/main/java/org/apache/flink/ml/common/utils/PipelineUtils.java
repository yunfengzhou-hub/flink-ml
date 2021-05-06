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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.function.EmbedStreamFunction;
import org.apache.flink.ml.common.function.StreamFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipelineUtils {
    /**
     * create a {@link StreamFunction} that represents the computation logic of the provided {@link Pipeline}.
     * This method does the following:
     * - Read Json string from the specified file path
     * - Use the Json string to initialize the pipeline
     * - Convert the computation logic of the pipeline into a Function.
     *
     * @param path_to_json_file path and file name of the json file that stores the json representation of the pipeline
     * @param <IN> Class of the input of the {@link Pipeline}
     * @param <OUT> Class of the output of the {@link Pipeline}
     * @return a {@link StreamFunction} with the computation logic
     * @throws Exception if the provided path is invalid or a function cannot be produced.
     */
    public static <IN, OUT> StreamFunction<IN, OUT> toFunction(String path_to_json_file, Class<IN> inClass, Class<OUT> outClass) throws Exception {
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

    public static <IN, OUT> StreamFunction<IN, OUT> toFunction(Pipeline pipeline, Class<IN> inClass, Class<OUT> outClass) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = createStreamTableEnvironment(env,settings);

        DataStream<IN> inStream = env.fromElements(inClass.getDeclaredConstructor().newInstance());
        Table input_table = tEnv.fromDataStream(inStream);

        Table output_table = pipeline.transform(tEnv, input_table);

        DataStream<OUT> outStream = tEnv.toAppendStream(output_table, outClass);

        return new EmbedStreamFunction<>(outStream);
    }

    public static <IN, OUT> StreamFunction<IN, OUT> toFunction(Pipeline pipeline, Class<IN> inClass, Class<OUT> outClass, boolean inStreamingMode) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
        if(inStreamingMode){
            builder.inStreamingMode();
        }else {
            builder.inBatchMode();
        }
        EnvironmentSettings settings = builder.useBlinkPlanner().build();
        StreamTableEnvironment tEnv = createStreamTableEnvironment(env,settings);

        DataStream<IN> inStream = env.fromElements(inClass.getDeclaredConstructor().newInstance());
        Table input_table = tEnv.fromDataStream(inStream);

        Table output_table = pipeline.transform(tEnv, input_table);

        DataStream<OUT> outStream = tEnv.toAppendStream(output_table, outClass);

        return new EmbedStreamFunction<>(outStream);
    }

    // a walkaround of StreamTableEnvironment's limit that StreamExecutionEnvironment cannot be executed in batch mode
    public static StreamTableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment executionEnvironment,
            EnvironmentSettings settings) {
        TableConfig tableConfig = new TableConfig();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        ModuleManager moduleManager = new ModuleManager();

        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(classLoader)
                        .config(tableConfig.getConfiguration())
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .executionConfig(executionEnvironment.getConfig())
                        .build();

        FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, catalogManager, moduleManager);

        Map<String, String> executorProperties = settings.toExecutorProperties();
        Executor executor = lookupExecutor(executorProperties, executionEnvironment);

        Map<String, String> plannerProperties = settings.toPlannerProperties();
        Planner planner =
                ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                        .create(
                                plannerProperties,
                                executor,
                                tableConfig,
                                functionCatalog,
                                catalogManager);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                tableConfig,
                executionEnvironment,
                planner,
                executor,
                settings.isStreamingMode(),
                classLoader);
    }

    private static Executor lookupExecutor(
            Map<String, String> executorProperties,
            StreamExecutionEnvironment executionEnvironment) {
        try {
            ExecutorFactory executorFactory =
                    ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor)
                    createMethod.invoke(executorFactory, executorProperties, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private static <OUT> List<Transformation<?>> table2Transformations(Table table, EnvironmentSettings settings, Class<OUT> clazz){
        TableEnvironmentImpl env = TableEnvironmentImpl.create(settings);
        Planner planner = env.getPlanner();

        TypeInformation<OUT> typeInfo = TypeExtractor.createTypeInfo(clazz);
        OutputConversionModifyOperation modifyOperation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        TypeConversions.fromLegacyInfoToDataType(typeInfo),
                        OutputConversionModifyOperation.UpdateMode.APPEND);

        List<Transformation<?>> transformations = planner.translate(Collections.singletonList(modifyOperation));
        return transformations;
    }
}
