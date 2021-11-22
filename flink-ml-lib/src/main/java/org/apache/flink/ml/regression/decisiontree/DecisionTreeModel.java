package org.apache.flink.ml.regression.decisiontree;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DecisionTreeModel implements Model<DecisionTreeModel>, DecisionTreeParams<DecisionTreeModel> {
    private Table modelTable;
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private static final String broadcastModelKey = "DecisionTreeModelStream";

    @Override
    public Table[] transform(Table... inputs) {
        List<String> colNames = new ArrayList<>(inputs[0].getResolvedSchema().getColumnNames());
        colNames.add(getPredictionCol());

        List<TypeInformation<?>> colTypes = new ArrayList<>(inputs[0].getResolvedSchema().getColumnDataTypes())
                .stream()
                .map((Function<DataType, TypeInformation<?>>) dataType -> TypeInformation.of(dataType.getConversionClass()))
                .collect(Collectors.toList());
        colTypes.add(TypeInformation.of(Double.class));

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) modelTable).getTableEnvironment();
        DataStream<DecisionTreeModelData> modelStream = tEnv.toDataStream(modelTable, DataTypes.RAW(DecisionTreeModelData.class));
        DataStream<Row> input = tEnv.toDataStream(inputs[0]);

        Map<String, DataStream<?>> broadcastMap = new HashMap<>();
        broadcastMap.put(broadcastModelKey, modelStream);

        Function<List<DataStream<?>>, DataStream<Row>> function = dataStreams -> {
            DataStream stream = dataStreams.get(0);
            return stream.transform(
                    this.getClass().getSimpleName(),
                    new RowTypeInfo(colTypes.toArray(new TypeInformation[0]), colNames.toArray(new String[0])),
                    new DecisionTreePredictOp(new DecisionTreePredictFunc())
            );
        };
        DataStream<Row> output = BroadcastUtils.withBroadcastStream(Collections.singletonList(input), broadcastMap, function);

        return new Table[]{tEnv.fromDataStream(output)};
    }

    @Override
    public void save(String path) throws IOException {

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void setModelData(Table... inputs) {
        modelTable = inputs[0];
    }

    private static class DecisionTreePredictOp
            extends AbstractUdfStreamOperator<Row, DecisionTreePredictFunc>
            implements OneInputStreamOperator<Row, Row> {
        public DecisionTreePredictOp(DecisionTreePredictFunc userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<Row> streamRecord) {
            output.collect(new StreamRecord<>(userFunction.map(streamRecord.getValue())));
        }
    }

    private static class DecisionTreePredictFunc extends RichMapFunction<Row, Row> {
        @Override
        public Row map(Row row) {
            DecisionTreeModelData modelData = (DecisionTreeModelData) getRuntimeContext().getBroadcastVariable(broadcastModelKey).get(0);
            Double label = modelData.root.predict((Vector) row.getField(0)).prediction;
            return Row.join(row, Row.of(label));
        }
    }
}
