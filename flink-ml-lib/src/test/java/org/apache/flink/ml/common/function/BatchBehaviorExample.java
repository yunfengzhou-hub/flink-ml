package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class BatchBehaviorExample {
    public static void main(String[] args) throws Exception {
        dataStreamStreamUnion();
        System.out.println("------");
        dataStreamBatchUnion();
        System.out.println("------");
        dataSetUnion();
        System.out.println("------");
        dataStreamBatchWindow();
        System.out.println("------");
        dataStreamBatchCountWindow();
        System.out.println("------");
        joinDuplicateKeys();
        System.out.println("------");
        joinDuplicateKeysStream();
        System.out.println("------");
        reduceBatch();
        System.out.println("------");
        reduceStream();
        System.out.println("------");
        batchState();
    }

    public static void dataStreamStreamUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Integer> stream2 = env.fromElements(1,4,5);
        DataStream<Integer> stream = stream1.union(stream2);
        stream.print();
        env.execute();
    }

    public static void dataStreamBatchUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Integer> stream2 = env.fromElements(1,4,5);
        DataStream<Integer> stream = stream1.union(stream2);
        stream.print();
        env.execute();
    }

    public static void dataSetUnion() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Integer> set1 = env.fromElements(1,2,3);
        DataSet<Integer> set2 = env.fromElements(1,4,5);
        DataSet<Integer> set = set1.union(set2).distinct();
        set.print();
//        env.execute();
    }

    public static void dataStreamBatchWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream = env.fromElements(1,2,3);
        DataStream<Integer> operation = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Integer>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(Integer integer) {
                        return integer;
                    }
                }).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                        int sum = 0;
                        for(int i:iterable){
                            sum += i;
                        }
                        collector.collect(sum);
                    }
                });
        operation.print();
        env.execute();
    }

    public static void dataStreamBatchCountWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream = env.fromElements(1,2,3,4);
        DataStream<Integer> operation = stream.keyBy(new KeySelector<Integer, Object>() {
            @Override
            public Object getKey(Integer integer) throws Exception {
                return integer<5;
            }
        }).countWindow(2).reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer integer, Integer t1) throws Exception {
                return integer + t1;
            }
        });
//                .apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
//                        int sum = 0;
//                        for(int i:iterable){
//                            sum += i;
//                        }
//                        collector.collect(sum);
//                    }
//                });
        operation.print();
        env.execute();
    }

    public static void joinDuplicateKeys() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream = env.fromElements(1,2,1,2)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Integer>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(Integer integer) {
                        return integer;
                    }
                });
        stream
                .join(stream)
                .where((KeySelector<Integer, Object>) integer -> integer)
                .equalTo((KeySelector<Integer, Object>) integer -> integer)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply((JoinFunction<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2).print();
        env.execute();
    }

    public static void joinDuplicateKeysStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStream<Integer> stream = env.fromElements(1,2,1,2)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Integer>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(Integer integer) {
                        return integer;
                    }
                });
        stream
                .join(stream)
                .where((KeySelector<Integer, Object>) integer -> integer)
                .equalTo((KeySelector<Integer, Object>) integer -> integer)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply((JoinFunction<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2).print();
        env.execute();
    }

    public static void reduceBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream = env.fromElements(1,2,1,2);
        stream.keyBy((KeySelector<Integer, Object>) integer -> integer)
                .reduce((ReduceFunction<Integer>) (integer, t1) -> integer + t1)
                .print();
        env.execute();
    }

    public static void reduceStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStream<Integer> stream = env.fromElements(1,2,1,2);
        stream.keyBy((KeySelector<Integer, Object>) integer -> integer)
                .reduce((ReduceFunction<Integer>) (integer, t1) -> integer + t1)
                .print();
        env.execute();
    }

    public static void batchState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> stream = env.fromElements(1,2,1,2);
        stream
                .keyBy((KeySelector<Integer, Object>) integer -> integer)
                .map(new RichMapFunction<Integer, Integer>() {
                    ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("state", Integer.class);

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
                })
                .print();
        env.execute();
    }
}
