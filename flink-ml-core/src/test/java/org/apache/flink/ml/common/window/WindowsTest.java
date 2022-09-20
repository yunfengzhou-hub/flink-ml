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

package org.apache.flink.ml.common.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.apache.commons.collections.IteratorUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link Window}s and {@link WindowUtils}. */
@SuppressWarnings("unchecked")
public class WindowsTest extends AbstractTestBase {
    private static final int RECORD_NUM = 100;

    private static List<Long> inputData;

    private static DataStream<Long> inputStream;
    private static DataStream<Long> inputStreamWithInterval;
    private static DataStream<Long> inputStreamWithTimestamp;

    @BeforeClass
    public static void before() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        inputData = new ArrayList<>();
        for (long i = 0; i < RECORD_NUM; i++) {
            inputData.add(i);
        }
        inputStream = env.fromCollection(inputData);

        inputStreamWithInterval =
                inputStream
                        .map(
                                new MapFunction<Long, Long>() {
                                    private int count = 0;

                                    @Override
                                    public Long map(Long value) throws Exception {
                                        count++;
                                        if (count % (RECORD_NUM / 2) == 0) {
                                            Thread.sleep(1000);
                                        }
                                        return value;
                                    }
                                })
                        .setParallelism(1);

        inputStreamWithTimestamp =
                inputStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Long>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Long>)
                                                (element, recordTimestamp) -> element));
    }

    @Test
    public void testBoundedWindow() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStream,
                        BoundedWindow.get(),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertEquals(1, actualBatches.size());
        assertEquals(new HashSet<>(inputData), new HashSet<>(actualBatches.get(0)));
    }

    @Test
    public void testCountWindow() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStream,
                        TumbleWindow.over(RECORD_NUM / 3),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertTrue(actualBatches.size() >= 3 && actualBatches.size() <= 4);
        int count = 0;
        for (List<Long> batch : actualBatches) {
            count += batch.size();
        }
        assertTrue(count >= 90 && count < 100);
    }

    @Test
    public void testTumbleWindowWithProcessTime() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStreamWithInterval,
                        TumbleWindow.over(Duration.ofMillis(100)).withProcessingTime(),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertTrue(actualBatches.size() > 1);
        List<Long> mergedBatches = new ArrayList<>();
        for (List<Long> batch : actualBatches) {
            mergedBatches.addAll(batch);
        }
        assertTrue(mergedBatches.containsAll(inputData.subList(0, RECORD_NUM - 1)));
    }

    @Test
    public void testTumblingWindowWithEventTime() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStreamWithTimestamp,
                        TumbleWindow.over(Duration.ofMillis(RECORD_NUM / 7)),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertEquals(8, actualBatches.size());
        List<Long> mergedBatches = new ArrayList<>();
        for (List<Long> batch : actualBatches) {
            mergedBatches.addAll(batch);
        }
        assertEquals(RECORD_NUM, mergedBatches.size());
        assertEquals(new HashSet<>(inputData), new HashSet<>(mergedBatches));
    }

    @Test
    public void testSessionWindowWithProcessTime() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStreamWithInterval,
                        SessionWindow.withGap(Duration.ofMillis(100)).withProcessingTime(),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertTrue(actualBatches.size() > 1);
        List<Long> mergedBatches = new ArrayList<>();
        for (List<Long> batch : actualBatches) {
            mergedBatches.addAll(batch);
        }
        assertTrue(mergedBatches.containsAll(inputData.subList(0, RECORD_NUM - 1)));
    }

    @Test
    public void testSessionWindowWithEventTime() throws Exception {
        DataStream<List<Long>> outputStream =
                WindowUtils.allWindowProcess(
                        inputStreamWithTimestamp,
                        SessionWindow.withGap(Duration.ofMillis(RECORD_NUM / 7)),
                        new CreateAllWindowBatchFunction<>(),
                        new ListTypeInfo<>(Long.class));
        List<List<Long>> actualBatches = IteratorUtils.toList(outputStream.executeAndCollect());
        assertEquals(1, actualBatches.size());
        assertEquals(new HashSet<>(inputData), new HashSet<>(actualBatches.get(0)));
    }

    private static class CreateAllWindowBatchFunction<IN>
            extends ProcessAllWindowFunction<IN, List<IN>, GlobalWindow> {
        @Override
        public void process(
                ProcessAllWindowFunction<IN, List<IN>, GlobalWindow>.Context context,
                Iterable<IN> elements,
                Collector<List<IN>> out) {
            List<IN> list = new ArrayList<>();
            elements.forEach(list::add);
            out.collect(list);
        }
    }
}
