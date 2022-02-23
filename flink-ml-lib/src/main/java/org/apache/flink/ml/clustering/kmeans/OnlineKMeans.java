package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.iteration.*;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class OnlineKMeans
        implements Estimator<OnlineKMeans, KMeansModel>, KMeansParams<OnlineKMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public OnlineKMeans() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public KMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> (DenseVector) row.getField(getFeaturesCol()));

        DataStream<DenseVector[]> initCentroids = selectRandomCentroids(points, getK(), getSeed());

        IterationBody body =
                new KMeansIterationBody(DistanceMeasure.getInstance(getDistanceMeasure()));

        DataStream<DenseVector[]> finalCentroids =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initCentroids), DataStreamList.of(points), body)
                        .get(0);

        Table finalCentroidsTable = tEnv.fromDataStream(finalCentroids.map(KMeansModelData::new));
        KMeansModel model = new KMeansModel().setModelData(finalCentroidsTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineKMeans load(StreamExecutionEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class KMeansIterationBody implements IterationBody {
        private final DistanceMeasure distanceMeasure;

        public KMeansIterationBody(DistanceMeasure distanceMeasure) {
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<DenseVector[]> centroids = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            DataStream<DenseVector[]> newCentroids =
                    points.connect(centroids.broadcast())
                            .flatMap(new SelectNearestCentroidOperator(distanceMeasure))
                            .map(new CountAppender())
                            .map(new CentroidAccumulator())
                            .map(new CentroidAverager());

            DataStream<DenseVector[]> finalCentroids =
                    newCentroids.filter(new ModelEvaluationFunction());

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids), DataStreamList.of(finalCentroids));
        }
    }

    private static class CentroidAverager
            implements MapFunction<List<Tuple3<Integer, DenseVector, Long>>, DenseVector[]> {
        @Override
        public DenseVector[] map(List<Tuple3<Integer, DenseVector, Long>> values) {
            for (Tuple3<Integer, DenseVector, Long> value : values) {
                for (int i = 0; i < value.f1.size(); i++) {
                    value.f1.values[i] /= value.f2;
                }
            }

            return values.stream().map(x -> x.f1).toArray(DenseVector[]::new);
        }
    }

    private static class CentroidAccumulator
            implements MapFunction<
                    List<Tuple3<Integer, DenseVector, Long>>,
                    List<Tuple3<Integer, DenseVector, Long>>> {
        @Override
        public List<Tuple3<Integer, DenseVector, Long>> map(
                List<Tuple3<Integer, DenseVector, Long>> tuple3s) throws Exception {
            Map<Integer, DenseVector> map1 = new HashMap<>();
            Map<Integer, Long> map2 = new HashMap<>();
            for (Tuple3<Integer, DenseVector, Long> tuple3 : tuple3s) {
                DenseVector vector =
                        map1.getOrDefault(tuple3.f0, new DenseVector(tuple3.f1.size()));
                long count = map2.getOrDefault(tuple3.f0, 0L);

                for (int i = 0; i < tuple3.f1.size(); i++) {
                    vector.values[i] += tuple3.f1.values[i];
                }
                count += tuple3.f2;

                map1.put(tuple3.f0, vector);
                map2.put(tuple3.f0, count);
            }

            List<Tuple3<Integer, DenseVector, Long>> list = new ArrayList<>();
            for (Integer key : map1.keySet()) {
                list.add(new Tuple3<>(key, map1.get(key), map2.get(key)));
            }
            return list;
        }
    }

    private static class CountAppender
            implements MapFunction<
                    List<Tuple2<Integer, DenseVector>>, List<Tuple3<Integer, DenseVector, Long>>> {
        @Override
        public List<Tuple3<Integer, DenseVector, Long>> map(
                List<Tuple2<Integer, DenseVector>> value) {
            return value.stream().map(x -> Tuple3.of(x.f0, x.f1, 1L)).collect(Collectors.toList());
        }
    }

    private static class SelectNearestCentroidOperator
            implements CoFlatMapFunction<
                    DenseVector, DenseVector[], List<Tuple2<Integer, DenseVector>>> {
        private final DistanceMeasure distanceMeasure;
        private DenseVector[] centroidValues;
        List<DenseVector> points = new ArrayList<>();
        List<DenseVector> pointsBuffer = new ArrayList<>();

        public SelectNearestCentroidOperator(DistanceMeasure distanceMeasure) {
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void flatMap1(
                DenseVector point, Collector<List<Tuple2<Integer, DenseVector>>> output)
                throws Exception {
            System.out.println("point");
            points.add(point);
            flatMap(output);
        }

        @Override
        public void flatMap2(
                DenseVector[] centroids, Collector<List<Tuple2<Integer, DenseVector>>> output)
                throws Exception {
            System.out.println("centroidValues");
            centroidValues = centroids;
            flatMap(output);
        }

        private void flatMap(Collector<List<Tuple2<Integer, DenseVector>>> output) {
            if (centroidValues == null || points.size() < 6) {
                return;
            }

            List<Tuple2<Integer, DenseVector>> results = new ArrayList<>();
            for (DenseVector point : points) {
                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;

                for (int i = 0; i < centroidValues.length; i++) {
                    DenseVector centroid = centroidValues[i];
                    double distance = distanceMeasure.distance(centroid, point);
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroidId = i;
                    }
                }
                results.add(Tuple2.of(closestCentroidId, point));
            }
            output.collect(results);

            points.clear();
            centroidValues = null;
        }
    }

    private static class ModelEvaluationFunction implements FilterFunction<DenseVector[]> {
        @Override
        public boolean filter(DenseVector[] denseVectors) throws Exception {
            System.out.println(Arrays.toString(denseVectors));
            return true;
        }
    }

    public static DataStream<DenseVector[]> selectRandomCentroids(
            DataStream<DenseVector> data, int k, long seed) {

        DataStream<DenseVector[]> resultStream =
                data.flatMap(new FlatMapFunction<DenseVector, DenseVector[]>() {
            boolean isSent = false;

            @Override
            public void flatMap(DenseVector denseVector, Collector<DenseVector[]> out) throws Exception {
                if (isSent) return;
                isSent = true;
                int size = denseVector.size();
                List<DenseVector> vectors = new ArrayList<>();
                Random random = new Random(seed);
                for (int i = 0; i < k; i ++) {
                    DenseVector vector = new DenseVector(size);
                    for (int j = 0; j < size; j ++) {
                        vector.values[j] = random.nextDouble();
                    }
                    vectors.add(vector);
                }
                Collections.shuffle(vectors, new Random(seed));
                out.collect(vectors.subList(0, k).toArray(new DenseVector[0]));
            }
        }).setParallelism(1);
        return resultStream;
    }
}
