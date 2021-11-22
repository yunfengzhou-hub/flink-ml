package org.apache.flink.ml.regression.decisiontree;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.common.iteration.OutputOnIterationTermination;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.tree.CategoricalSplit;
import org.apache.flink.ml.tree.Split;
import org.apache.flink.ml.tree.impurity.Entropy;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class DecisionTree implements Estimator<DecisionTree, DecisionTreeModel>,
        DecisionTreeParams<DecisionTree> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private static final String findMatchNodeKey = "FindMatchNode";
    private static final double EPSILON = 1e-4;
    private static final int MAX_DEPTH = 5;

    @Override
    public DecisionTreeModel fit(Table... inputs) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Vector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> (Vector) row.getField(getVarianceCol()));
        StreamExecutionEnvironment env = points.getExecutionEnvironment();

        DataStream<Tuple2<Split, Integer>> initModel = env.fromCollection(Collections.emptyList());

        IterationConfig config =
                IterationConfig.newBuilder()
                        .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.PER_ROUND)
                        .build();

        IterationBody body = new DecisionTreeIterationBody();

        DataStream<Tuple2<Split, Integer>> finalCentroids =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(initModel),
                        ReplayableDataStreamList.notReplay(points),
                        config,
                        body)
                        .get(0);

        DecisionTreeModel model = new DecisionTreeModel();
        model.setModelData(tEnv.fromDataStream(finalCentroids));
        ReadWriteUtils.setStageParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class DecisionTreeIterationBody implements IterationBody {

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Vector> data = dataStreams.get(0);
            DataStream<Tuple2<Split, Integer>> model = variableStreams.get(0);

            Map<String, DataStream<?>> broadcastMap = new HashMap<>();
            broadcastMap.put(findMatchNodeKey, model);

            Function<List<DataStream<?>>, DataStream<Tuple2<Vector, Integer>>> function = streams -> {
                DataStream stream = streams.get(0);
                return stream.transform(
                        this.getClass().getSimpleName(),
                        new TupleTypeInfo<Tuple2<Vector, Integer>>(TypeInformation.of(Vector.class), TypeInformation.of(Integer.class)),
                        new FindMatchNodeOp(new FindMatchNodeFunc())
                );
            };

            DataStream<Tuple2<Vector, Integer>> dataWithNodeId =
                    BroadcastUtils.withBroadcastStream(Collections.singletonList(data), broadcastMap, function);

            DataStream<Tuple2<Double, Integer>> impurityWithNodeId = dataWithNodeId
                    .keyBy((KeySelector<Tuple2<Vector, Integer>, Object>) value -> value.f1)
                    .window(EndOfStreamWindows.get())
                    .aggregate(new CalculateImpurityFunction());

            DataStream<Integer> terminationCriteria = impurityWithNodeId
                    .windowAll(EndOfStreamWindows.get())
                    .reduce((ReduceFunction<Tuple2<Double, Integer>>) (t0, t1) -> new Tuple2<>(Math.max(t0.f0, t1.f0), Math.min(t0.f1, t1.f1)))
                    .filter((FilterFunction<Tuple2<Double, Integer>>) x -> x.f0 < EPSILON || x.f1 >= Math.pow(MAX_DEPTH, 2))
                    .map((MapFunction<Tuple2<Double, Integer>, Integer>) x -> 0);

            impurityWithNodeId = impurityWithNodeId.filter(new FilterSplitFunciton());

            dataWithNodeId = dataWithNodeId.join(impurityWithNodeId)
                    .where((KeySelector<Tuple2<Vector, Integer>, Object>) x -> x.f1)
                    .equalTo((KeySelector<Tuple2<Double, Integer>, Object>) x -> x.f1)
                    .window(EndOfStreamWindows.get())
                    .apply((JoinFunction<Tuple2<Vector, Integer>, Tuple2<Double, Integer>, Tuple2<Vector, Integer>>) (x, y) -> x);

            DataStream<Tuple2<Split, Integer>> newModelStream = dataWithNodeId
                    .keyBy((KeySelector<Tuple2<Vector, Integer>, Object>) value -> value.f1)
                    .window(EndOfStreamWindows.get())
                    .aggregate(new FindBestSplitFunc())
                    .union(model);
            DataStream<Tuple2<Split, Integer>> finalModelStream = newModelStream
                    .flatMap(new OutputOnIterationTermination<>());

            return new IterationBodyResult(
                    DataStreamList.of(newModelStream),
                    DataStreamList.of(finalModelStream),
                    terminationCriteria
            );
        }
    }

    private static class FindMatchNodeOp
            extends AbstractUdfStreamOperator<Row, FindMatchNodeFunc>
            implements OneInputStreamOperator<Row, Row> {
        public FindMatchNodeOp(FindMatchNodeFunc userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<Row> streamRecord) {
            output.collect(new StreamRecord<>(userFunction.map(streamRecord.getValue())));
        }
    }

    private static class FindMatchNodeFunc extends RichMapFunction<Row, Row> {
        @Override
        public Row map(Row row) {
            List<?> modelData = getRuntimeContext().getBroadcastVariable(findMatchNodeKey);
            int node = findNode(modelData, (Vector) row.getField(0));
            return Row.join(row, Row.of(node));
        }
    }

    private static int findNode(List<?> modelData, Vector features) {
        Map<Integer, Split> map = toMap(modelData);
        int node = 1;
        while(map.containsKey(node)) {
            if (map.get(node).shouldGoLeft(features)) {
                node = node * 2;
            } else {
                node = node * 2 + 1;
            }
        }
        return node;
    }

    private static Map<Integer, Split> toMap(List<?> modelData) {
        Map<Integer, Split> map = new HashMap<>();
        for (Object o: modelData) {
            Tuple2<Integer, Split> tuple = (Tuple2<Integer, Split>) o;
            map.put(tuple.f0, tuple.f1);
        }

        return map;
    }

    private static class FindBestSplitFunc implements
            AggregateFunction<Tuple2<Vector, Integer>, Tuple2<List<Vector>, Integer>, Tuple2<Split, Integer>> {

        @Override
        public Tuple2<List<Vector>, Integer> createAccumulator() {
            return new Tuple2<>(new ArrayList<>(), -1);
        }

        @Override
        public Tuple2<List<Vector>, Integer> add(Tuple2<Vector, Integer> value, Tuple2<List<Vector>, Integer> acc) {
            acc.f0.add(value.f0);
            acc.f1 = value.f1;
            return acc;
        }

        @Override
        public Tuple2<Split, Integer> getResult(Tuple2<List<Vector>, Integer> acc) {
            int featureNum = acc.f0.get(0).size();
            Split bestSplit = null;
            double bestGain = 0.0;
            for (int i = 0; i < featureNum; i++) {
                Map<Double, Integer> featureFreqMap = new HashMap<>();
                for (Vector vector: acc.f0) {
                    featureFreqMap.put(vector.get(i), featureFreqMap.getOrDefault(vector.get(i), 0) + 1);
                }
                List<Integer> counts = new ArrayList<>();
                List<Double> categories = new ArrayList<>();
                int totalCount = 0;
                for (Map.Entry<Double, Integer> entry: featureFreqMap.entrySet()) {
                    counts.add(entry.getValue());
                    categories.add(entry.getKey());
                    totalCount += entry.getValue();

                    double gain = new Entropy().calculate(counts.stream().mapToDouble(x -> (double)x).toArray(), totalCount);
                    if (gain > bestGain) {
                        bestGain = gain;
                        bestSplit = new CategoricalSplit(true, i, categories.toArray(new Double[0]));
                    }
                }
            }
            return new Tuple2<>(bestSplit, acc.f1);
        }

        @Override
        public Tuple2<List<Vector>, Integer> merge(Tuple2<List<Vector>, Integer> acc, Tuple2<List<Vector>, Integer> acc1) {
            acc.f0.addAll(acc1.f0);
            if (acc1.f1 != -1) {
                acc.f1 = acc1.f1;
            }
            return acc;
        }
    }

    private static class CalculateImpurityFunction
            implements AggregateFunction<Tuple2<Vector, Integer>, Tuple2<Map<Vector, Integer>, Integer>, Tuple2<Double, Integer>> {
        @Override
        public Tuple2<Map<Vector, Integer>, Integer> createAccumulator() {
            return new Tuple2<>(new HashMap<>(), -1);
        }

        @Override
        public Tuple2<Map<Vector, Integer>, Integer> add(Tuple2<Vector, Integer> value, Tuple2<Map<Vector, Integer>, Integer> acc) {
            acc.f0.put(value.f0, acc.f0.getOrDefault(value.f0, 0) + 1);
            acc.f1 = value.f1;
            return acc;
        }

        @Override
        public Tuple2<Double, Integer> getResult(Tuple2<Map<Vector, Integer>, Integer> acc) {
            double[] counts = new double[acc.f0.size()];
            double totalCount = 0.0;
            int index = 0;
            for (Map.Entry<Vector, Integer> entry: acc.f0.entrySet()) {
                counts[index++] = entry.getValue();
                totalCount += entry.getValue();
            }
            double result = new Entropy().calculate(counts, totalCount);
            return new Tuple2<>(result, acc.f1);
        }

        @Override
        public Tuple2<Map<Vector, Integer>, Integer> merge(Tuple2<Map<Vector, Integer>, Integer> acc, Tuple2<Map<Vector, Integer>, Integer> acc1) {
            for (Map.Entry<Vector, Integer> entry: acc1.f0.entrySet()) {
                acc.f0.put(entry.getKey(), acc.f0.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
            if (acc1.f1 != -1) {
                acc.f1 = acc1.f1;
            }
            return acc;
        }
    }

    private static class FilterSplitFunciton implements FilterFunction<Tuple2<Double, Integer>> {

        @Override
        public boolean filter(Tuple2<Double, Integer> value) throws Exception {
            return value.f0 >= EPSILON && value.f1 < Math.pow(MAX_DEPTH, 2);
        }
    }
}
