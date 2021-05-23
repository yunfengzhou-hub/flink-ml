package org.apache.flink.ml.common.function.tomcat;

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.function.StreamFunction;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.PrintWriter;

public class TrainModel {
    static DataStream<MenuItem> stream;
    public static void main(String[] args) throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new MyEstimator());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        stream = env.fromElements(new MenuItem());
        Table table = tEnv.fromDataStream(stream);

        Pipeline fittedPipeline = pipeline.fit(tEnv, table);

        StreamFunction<MenuItem, MenuItem> function = PipelineUtils.toFunction(
                fittedPipeline, env, tEnv, MenuItem.class, MenuItem.class
        );

        PrintWriter out = new PrintWriter("/tmp/model.txt");
        out.println(PipelineUtils.serializeFunction(function));
        out.close();
    }

    public static class MyEstimator implements Estimator<MyEstimator, MyModel> {

        @Override
        public MyModel fit(TableEnvironment tableEnvironment, Table table) {
            return new MyModel();
        }

        @Override
        public Params getParams() {
            return new Params();
        }
    }

    public static class MyModel implements Model<MyModel> {

        @Override
        public Table transform(TableEnvironment tableEnvironment, Table table) {
            return table;
        }

        @Override
        public Params getParams() {
            return new Params();
        }
    }
}
