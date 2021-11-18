package org.apache.flink.ml.feature.onehotencoder;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OneHotEncoder implements Estimator<OneHotEncoder, OneHotEncoderModel>, OneHotEncoderParams<OneHotEncoder> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public OneHotEncoder() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public OneHotEncoderModel fit(Table... inputs) {
        final String[] inputCols = getInputCols();

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Tuple2<Integer, Integer>> modelDataStream = tEnv.toDataStream(inputs[0])
                .flatMap(new ExtractFeatureFunction(inputCols))
                .keyBy(x -> x.f0)
                .window(EndOfStreamWindows.get())
                .reduce((x, y) -> new Tuple2<>(x.f0, Math.max(x.f1, y.f1)));

        OneHotEncoderModel model = new OneHotEncoderModel().setModelData(OneHotEncoderModelData.fromDataStream(tEnv, modelDataStream));
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OneHotEncoder load(StreamExecutionEnvironment env, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class ExtractFeatureFunction implements FlatMapFunction<Row, Tuple2<Integer, Integer>> {
        private final String[] inputCols;

        private ExtractFeatureFunction(String[] inputCols) {
            this.inputCols = inputCols;
        }

        @Override
        public void flatMap(Row row, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            Number number;
            for (int i = 0; i < inputCols.length; i++) {
                number = (Number) row.getField(inputCols[i]);
                Preconditions.checkArgument(number.intValue() == number.doubleValue());
                collector.collect(new Tuple2<>(i, number.intValue()));
            }
        }
    }
}
