package org.apache.flink.ml.common.function;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SetStreamExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(sEnv);
        ExecutionEnvironment bEnv = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment btEnv = BatchTableEnvironment.create(bEnv);

        DataStream<TestType.Order> stream = sEnv.fromElements(new TestType.Order());
        Table table = stEnv.fromDataStream(stream);
        DataSet<TestType.Order> set = btEnv.toDataSet(table, TestType.Order.class);

    }
}
