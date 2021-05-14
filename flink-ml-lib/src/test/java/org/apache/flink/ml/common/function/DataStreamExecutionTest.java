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

package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class DataStreamExecutionTest {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    @Before
    public void createEnvironment() {
        env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }

    @Test
    public void testBasicCorrectness() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<String, String>(stream).apply("hello"));
    }

    @Test
    public void testChainedOperation() throws Exception {
        DataStream<String> stream = env.fromElements(1)
                .map(x -> x + "x")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("1X"), new EmbedStreamFunction<>(stream).apply(1));
    }

    @Test
    public void testConnectedStream() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).map(new CoMapFunction<Integer, Integer, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        assertEquals(Arrays.asList(2,2), new EmbedStreamFunction<>(connected).apply(1));
    }

    @Test
    public void testUnionStream() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> unioned = branch1.union(branch2).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        assertEquals(Arrays.asList(2,2), new EmbedStreamFunction<>(unioned).apply(1));
    }

    @Test
    public void testFlatMap() throws Exception {
        DataStream<Integer> stream = env.fromElements(1)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(
                            Integer integer,
                            Collector<Integer> collector) throws Exception {
                        collector.collect(integer+1);
                        collector.collect(integer+1);
                    }
                });

        assertEquals(Arrays.asList(2,2), new EmbedStreamFunction<>(stream).apply(1));
    }

    @Test
    public void testFilter() throws Exception {
        DataStream<Integer> stream = env.fromElements(1)
                .filter((FilterFunction<Integer>) integer -> false);

        assertEquals(Collections.emptyList(), new EmbedStreamFunction<>(stream).apply(1));
    }

    @Test
    public void testCoFlatMap() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).flatMap(new CoFlatMapFunction<Integer, Integer, Integer>() {
            @Override
            public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }

            @Override
            public void flatMap2(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }
        });
        assertEquals(Arrays.asList(2, 2), new EmbedStreamFunction<>(connected).apply(1));
    }

    @Test
    public void testProject() throws Exception {
        DataStream<Tuple3<String, Integer, Boolean>> input = env.fromElements(new Tuple3<>("hello", 1, true));
        DataStream<Tuple2<Boolean, String>> stream = input.project(2, 0);

        assertEquals(Collections.singletonList(new Tuple2<>(true, "hello")), new EmbedStreamFunction<>(stream).apply(new Tuple3<>("hello", 1, true)));
    }

    @Test
    public void testStartNewChain() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .startNewChain()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
    }

    @Test
    public void testDisableChaining() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .disableChaining()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
    }

    @Test
    public void testNotClearOperators() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
    }

    @Test
    public void testMultipleInvocation() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        EmbedStreamFunction<String, String> function = new EmbedStreamFunction<>(stream);
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
    }

    @Test
    public void testOperatorID() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase).uid("operator 6")
                .map(String::toLowerCase).uid("operator 5")
                .map(String::toUpperCase).uid("operator 4")
                .map(String::toLowerCase).uid("operator 3")
                .map(String::toUpperCase).uid("operator 2")
                .map(String::toLowerCase).uid("operator 1")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
    }

    @Test
    public void testIrrelevantBranch() throws Exception {
        DataStream<String> input = env.fromElements("hello");
        DataStream<String> stream = input.map(String::toUpperCase);
        DataStream<String> stream2 = stream.map(x -> x+" " +x);

        input.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream2.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
        assertEquals(Collections.singletonList("HELLO HELLO"), new EmbedStreamFunction<>(stream2).apply("hello"));
    }

    @Test
    public void testPartitioning() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .shuffle()
                .map(String::toUpperCase);

        assertEquals(Collections.singletonList("HELLO"), new EmbedStreamFunction<>(stream).apply("hello"));
    }

    @Test
    public void testMixTable() throws Exception {
        DataStream<TestType.Order> stream = env.fromElements(new TestType.Order())
                .map((MapFunction<TestType.Order, TestType.Order>) order -> {
                    order.amount ++;
                    return order;
                });
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream)
                .select($("user").plus(1L).as("user"), $("amount"), $("product"));
        DataStream<TestType.Order> outStream = tEnv.toAppendStream(table, TestType.Order.class);

        TestType.Order inputData = new TestType.Order(
                1L,
                "product",
                1L
        );
        TestType.Order outputData = new TestType.Order(
                2L,
                "product",
                2L
        );

        assertEquals(Collections.singletonList(outputData), new EmbedStreamFunction<>(outStream).apply(inputData));
    }

    @Test(expected = Exception.class)
    public void testRuntimeError() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map((MapFunction<String, String>) s -> s.substring(10));
        new EmbedStreamFunction<>(stream).apply("hello");
    }

    @Test
    public void testState() throws Exception {
        DataStream<Integer> stream = env.fromElements(0);
        DataStream<Integer> stream2 = stream.union(stream)
                .keyBy((KeySelector<Integer, Object>) x -> x)
                .map(new RichMapFunction<Integer, Integer>() {
                    final ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("state", Integer.class);

                    @Override
                    public Integer map(Integer integer) throws Exception {
                        ValueState<Integer> val = getRuntimeContext().getState(descriptor);
                        if(val.value() == null){
                            val.update(integer);
                        }else{
                            val.update(val.value() + integer);
                        }
                        return val.value();
                    }
                });
        assertEquals(Arrays.asList(1, 2), new EmbedStreamFunction<>(stream2).apply(1));
    }

    @Test
    public void testKeyBy() throws Exception {
        DataStream<String> stream = env.fromElements("test");
        DataStream<String> stream2 = stream.union(stream)
                .keyBy((KeySelector<String, Object>) String::length)
                .reduce((ReduceFunction<String>) (s, t1) -> s+t1);
        assertEquals(Arrays.asList("hello", "hellohello"), new EmbedStreamFunction<>(stream2).apply("hello"));
    }

    @Test
    public void testCountWindow() throws Exception {
        DataStream<Integer> stream = env.fromElements(0);
        DataStream<Integer> stream2 = stream.union(stream)
                .keyBy((KeySelector<Integer, Object>) x -> x)
                .countWindow(2)
                .reduce((ReduceFunction<Integer>) Integer::sum);
        assertEquals(Arrays.asList(2), new EmbedStreamFunction<>(stream2).apply(1));
    }

    @After
    public void clearGraph() {
        env.getStreamGraph();
    }
}
