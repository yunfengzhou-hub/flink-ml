package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class ExampleTest {
    @Test
    public void testUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

        StreamTableEnvironment tEnv = createStreamTableEnvironment(env);
        DataStream<TestType.Order> input = env.fromElements(new TestType.Order());
        Table table = tEnv.fromDataStream(input);

        Table table1 = table.select($("user"), $("product").upperCase().as("product"), $("amount"));
//                return table1;
        Table table2 = table.unionAll(table)
                .groupBy(
//                                $("user"),
                        $("product")
                ).select(
                        $("user")
                                .sum()
                                .as("user"),
                        $("product"),
                        $("amount")
                                .sum()
                                .as("amount")
                );

//        DataStream<TestType.Order> output = tEnv.toRetractStream(table2, TestType.Order.class)
//                .map(new MapFunction<Tuple2<Boolean, TestType.Order>, TestType.Order>() {
//                    @Override
//                    public TestType.Order map(Tuple2<Boolean, TestType.Order> booleanOrderTuple2) throws Exception {
//                        return booleanOrderTuple2.f1;
//                    }
//                });
        DataStream<TestType.Order> output = tEnv.toAppendStream(table2, TestType.Order.class);
        output.print();
        env.execute();
    }

    @Test
    public void testUnionSelect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = createStreamTableEnvironment(env);
        DataStream<TestType.Order> input = env.fromElements(new TestType.Order());
//        input.print()
        Table table = tEnv.fromDataStream(input);
        table.printSchema();

        Table table1 = table.select(
                $("user").times(3).as("user"),
                $("product"),
                $("amount").times(10).as("amount")
        );
        Table table2 = table.unionAll(table1);
        DataStream<TestType.Order> output = tEnv.toAppendStream(table2, TestType.Order.class);
        output.print();
        env.execute();
    }

    @Test
    public void testTableBug() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv =StreamTableEnvironment.create(env);
        DataStream<TestType.Order> input = env.fromElements(new TestType.Order());
        Table table = tEnv.fromDataStream(input);

        Table table1 = table.select(
                $("user").times(3).as("user"),
                $("product"),
                $("amount").times(2).as("amount")
        );
        Table table2 = table.unionAll(table1);
        DataStream<TestType.Order> output = tEnv.toAppendStream(table2, TestType.Order.class);
        output.print();
        env.execute();
    }

    @Test
    public void testOrderByOffset() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("amount"), $("product"), $("user").plus(1).as("user"));
                Table table2 = table.select($("amount"), $("product"), $("user").plus(2).as("user"));
                return table.union(table1).union(table2);
            }
        });
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.orderBy($("user").asc()).offset(2);
            }
        });


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tEnv = createStreamTableEnvironment(env);
        DataStream<TestType.Order> input = env.fromElements(new TestType.Order());
        Table table = tEnv.fromDataStream(input);
        Table table1 = pipeline.transform(tEnv, table);

        DataStream<TestType.Order> output = tEnv.toAppendStream(table1, TestType.Order.class);
        output.print();
        env.execute();

//        TestType.Order data1 = new TestType.Order(1L, "product", 1L);
//        TestType.Order data2 = new TestType.Order(2L, "product", 1L);
//        TestType.Order data3 = new TestType.Order(3L, "product", 1L);
//
//        assertEquals(Arrays.asList(data3), function.apply(data1));
    }

    @Test
    public void testCollectionEnvironment() throws Exception {
        ExecutionEnvironment env = new CollectionEnvironment();
        DataSet<String> set = env.fromElements("hello").map(String::toUpperCase);

        int repeat = 10000;
        long duration = 0;
        long time;
        for(int i = 1; i<repeat;i++){
            time = System.currentTimeMillis();
            set.print();
            duration += System.currentTimeMillis() - time;
        }
        System.out.println((double) duration / (double) repeat);
    }

    @Test
    public void testLocalEnvironment() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<String> set = env.fromElements("hello").map(String::toUpperCase);

        int repeat = 100;
        long duration = 0;
        long time;
        for(int i = 1; i<repeat;i++){
            time = System.currentTimeMillis();
            set.print();
            duration += System.currentTimeMillis() - time;
        }
        System.out.println((double) duration / (double) repeat);
    }

    @Test
    public void testStreamFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello").map(String::toUpperCase);
        StreamFunction<String, String> func = new EmbedStreamFunction<>(stream);

        int repeat = 10000;
        long duration = 0;
        long time;
        for(int i = 1; i<repeat;i++){
            time = System.currentTimeMillis();
            func.apply("hello");
            duration += System.currentTimeMillis() - time;
        }
        System.out.println((double) duration / (double) repeat);
    }

    @Test
    public void testLocalStreamEnvironment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello").map(String::toUpperCase);
//        stream.print();

        int repeat = 100;
        long duration = 0;
        long time;
        for(int i = 1; i<repeat;i++){
            time = System.currentTimeMillis();
            stream.executeAndCollect();
            duration += System.currentTimeMillis() - time;
        }
        System.out.println((double) duration / (double) repeat);
    }

    public static StreamTableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment executionEnvironment) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableConfig tableConfig = new TableConfig();

        // temporary solution until FLINK-15635 is fixed
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
}
