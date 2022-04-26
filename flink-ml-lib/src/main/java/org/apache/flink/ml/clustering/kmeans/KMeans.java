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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.iteration.ForwardInputsOfLastRound;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * An Estimator which implements the k-means clustering algorithm.
 *
 * <p>See https://en.wikipedia.org/wiki/K-means_clustering.
 */
public class KMeans implements Estimator<KMeans, KMeansModel>, KMeansParams<KMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeans() {
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

        IterationConfig config =
                IterationConfig.newBuilder()
                        .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                        .build();

        IterationBody body =
                new KMeansIterationBody(
                        getMaxIter(), DistanceMeasure.getInstance(getDistanceMeasure()));

        DataStream<KMeansModelData> finalModelData =
                Iterations.iterateBoundedStreamsUntilTermination(
                                DataStreamList.of(initCentroids),
                                ReplayableDataStreamList.notReplay(points),
                                config,
                                body)
                        .get(0);

        Table finalModelDataTable = tEnv.fromDataStream(finalModelData);
        KMeansModel model = new KMeansModel().setModelData(finalModelDataTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static KMeans load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class KMeansIterationBody implements IterationBody {
        private final int maxIterationNum;
        private final DistanceMeasure distanceMeasure;

        public KMeansIterationBody(int maxIterationNum, DistanceMeasure distanceMeasure) {
            this.maxIterationNum = maxIterationNum;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<DenseVector[]> centroids = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            DataStream<Integer> terminationCriteria =
                    centroids.flatMap(new TerminateOnMaxIter(maxIterationNum));

            DataStream<Tuple2<Integer, DenseVector>> centroidIdAndPoints =
                    points.connect(centroids.broadcast())
                            .transform(
                                    "SelectNearestCentroid",
                                    new TupleTypeInfo<>(
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            DenseVectorTypeInfo.INSTANCE),
                                    new SelectNearestCentroidOperator(distanceMeasure));

            PerRoundSubBody perRoundSubBody =
                    new PerRoundSubBody() {
                        @Override
                        public DataStreamList process(DataStreamList inputs) {
                            DataStream<Tuple2<Integer, DenseVector>> centroidIdAndPoints =
                                    inputs.get(0);
                            DataStream<KMeansModelData> modelDataStream =
                                    centroidIdAndPoints
                                            .map(new CountAppender())
                                            .keyBy(t -> t.f0)
                                            .window(EndOfStreamWindows.get())
                                            .reduce(new CentroidAccumulator())
                                            .map(new CentroidAverager())
                                            .windowAll(EndOfStreamWindows.get())
                                            .apply(new ModelDataGenerator());
                            return DataStreamList.of(modelDataStream);
                        }
                    };
            DataStream<KMeansModelData> newModelData =
                    IterationBody.forEachRound(
                                    DataStreamList.of(centroidIdAndPoints), perRoundSubBody)
                            .get(0);

            DataStream<DenseVector[]> newCentroids =
                    newModelData.map(x -> x.centroids).setParallelism(1);

            DataStream<KMeansModelData> finalModelData =
                    newModelData.flatMap(new ForwardInputsOfLastRound<>());

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids),
                    DataStreamList.of(finalModelData),
                    terminationCriteria);
        }
    }

    private static class ModelDataGenerator
            implements AllWindowFunction<Tuple2<DenseVector, Double>, KMeansModelData, TimeWindow> {
        @Override
        public void apply(
                TimeWindow timeWindow,
                Iterable<Tuple2<DenseVector, Double>> iterable,
                Collector<KMeansModelData> collector) {
            List<Tuple2<DenseVector, Double>> list = IteratorUtils.toList(iterable.iterator());
            DenseVector[] centroids = new DenseVector[list.size()];
            DenseVector weights = new DenseVector(list.size());
            for (int i = 0; i < list.size(); i++) {
                centroids[i] = list.get(i).f0;
                weights.values[i] = list.get(i).f1;
            }
            collector.collect(new KMeansModelData(centroids, weights));
        }
    }

    private static class CentroidAverager
            implements MapFunction<
                    Tuple3<Integer, DenseVector, Long>, Tuple2<DenseVector, Double>> {
        @Override
        public Tuple2<DenseVector, Double> map(Tuple3<Integer, DenseVector, Long> value) {
            for (int i = 0; i < value.f1.size(); i++) {
                value.f1.values[i] /= value.f2;
            }
            return Tuple2.of(value.f1, value.f2.doubleValue());
        }
    }

    private static class CentroidAccumulator
            implements ReduceFunction<Tuple3<Integer, DenseVector, Long>> {
        @Override
        public Tuple3<Integer, DenseVector, Long> reduce(
                Tuple3<Integer, DenseVector, Long> v1, Tuple3<Integer, DenseVector, Long> v2) {
            for (int i = 0; i < v1.f1.size(); i++) {
                v1.f1.values[i] += v2.f1.values[i];
            }
            return new Tuple3<>(v1.f0, v1.f1, v1.f2 + v2.f2);
        }
    }

    private static class CountAppender
            implements MapFunction<
                    Tuple2<Integer, DenseVector>, Tuple3<Integer, DenseVector, Long>> {
        @Override
        public Tuple3<Integer, DenseVector, Long> map(Tuple2<Integer, DenseVector> value) {
            return Tuple3.of(value.f0, value.f1, 1L);
        }
    }

    private static class SelectNearestCentroidOperator
            extends AbstractStreamOperator<Tuple2<Integer, DenseVector>>
            implements TwoInputStreamOperator<
                            DenseVector, DenseVector[], Tuple2<Integer, DenseVector>>,
                    IterationListener<Tuple2<Integer, DenseVector>> {
        private final DistanceMeasure distanceMeasure;
        private ListState<DenseVector[]> centroidsState;
        private DenseVector[] centroids;

        private Path basePath;
        private StreamConfig config;
        private StreamTask<?, ?> containingTask;
        private DataCacheWriter<DenseVector> dataCacheWriter;
        //        private byte[] dataBufferArray = null;
        //        private final List<DenseVector> dataBuffer = new ArrayList<>(400000);
        //        private OutputStream dataBuffer;
        //        private DataOutputViewStreamWrapper dataBufferView;
        //        private ObjectOutputStream objectOutputStream;
        //        private int count = 0;
        //        private boolean isFlush = false;

        public SelectNearestCentroidOperator(DistanceMeasure distanceMeasure) {
            super();
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Tuple2<Integer, DenseVector>>> output) {
            super.setup(containingTask, config, output);

            basePath =
                    OperatorUtils.getDataCachePath(
                            containingTask.getEnvironment().getTaskManagerInfo().getConfiguration(),
                            containingTask
                                    .getEnvironment()
                                    .getIOManager()
                                    .getSpillingDirectoriesPaths());

            this.config = config;
            //            this.containingTask = containingTask;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE);
            centroidsState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("centroids", type));
            centroids = null;

            basePath =
                    OperatorUtils.createDataCacheFileGenerator(
                                    basePath, "cache", config.getOperatorID())
                            .get();
            //            dataBuffer =
            //                    basePath.getFileSystem().create(basePath,
            // FileSystem.WriteMode.NO_OVERWRITE);
            //
            //            //            basePath =
            //            //                    new Path(
            //            //                            "/tmp/ml-benchmark"
            //            //                                    + System.currentTimeMillis()
            //            //                                    +
            // getRuntimeContext().getIndexOfThisSubtask());
            //            //            dataBuffer =
            //            //                    basePath.getFileSystem().create(basePath,
            //            // FileSystem.WriteMode.NO_OVERWRITE);
            //
            //            //            dataBuffer = new ByteArrayOutputStream();
            //            //            dataBufferView = new
            // DataOutputViewStreamWrapper(dataBuffer);
            //            objectOutputStream = new ObjectOutputStream(dataBuffer);

            dataCacheWriter =
                    new DataCacheWriter<>(
                            DenseVectorSerializer.INSTANCE,
                            basePath.getFileSystem(),
                            OperatorUtils.createDataCacheFileGenerator(
                                    basePath, "cache", config.getOperatorID()));
        }

        @Override
        public void processElement1(StreamRecord<DenseVector> streamRecord) throws Exception {
            dataCacheWriter.addRecord(streamRecord.getValue());
            //            DenseVectorSerializer.INSTANCE.serialize(streamRecord.getValue(),
            // dataBufferView);
            //            objectOutputStream.writeObject(streamRecord.getValue());
            //            dataBuffer.add(streamRecord.getValue());
            //            count++;
            //            isFlush = true;
            if (centroids != null) {
                DenseVector point = streamRecord.getValue();
                int closestCentroidId = findClosestCentroidId(centroids, point, distanceMeasure);
                output.collect(new StreamRecord<>(Tuple2.of(closestCentroidId, point)));
            }
        }

        @Override
        public void processElement2(StreamRecord<DenseVector[]> streamRecord) throws Exception {
            Preconditions.checkState(centroids == null);
            centroidsState.add(streamRecord.getValue());
            centroids = streamRecord.getValue();

            //            for (DenseVector point : dataBuffer) {
            //                int closestCentroidId = findClosestCentroidId(centroids, point,
            // distanceMeasure);
            //                output.collect(new StreamRecord<>(Tuple2.of(closestCentroidId,
            // point)));
            //            }

            //            DataInputView bInput =
            //                    new DataInputViewStreamWrapper(
            //                            new ByteArrayInputStream(dataBuffer.toByteArray()));

            //            if (isFlush) {
            //                dataBuffer.flush();
            //                isFlush = false;
            //            }
            //            DataInputView bInput =
            //                    new
            // DataInputViewStreamWrapper(basePath.getFileSystem().open(basePath));
            //
            //            for (int i = 0; i < count; i++) {
            //                DenseVector point =
            // DenseVectorSerializer.INSTANCE.deserialize(bInput);
            //                int closestCentroidId = findClosestCentroidId(centroids, point,
            // distanceMeasure);
            //                output.collect(new StreamRecord<>(Tuple2.of(closestCentroidId,
            // point)));
            //            }

            //            objectOutputStream.flush();
            //            ObjectInputStream inputStream =
            //                    new ObjectInputStream(basePath.getFileSystem().open(basePath));
            //            for (int i = 0; i < count; i++) {
            //                DenseVector point = (DenseVector) inputStream.readObject();
            //                int closestCentroidId = findClosestCentroidId(centroids, point,
            // distanceMeasure);
            //                output.collect(new StreamRecord<>(Tuple2.of(closestCentroidId,
            // point)));
            //            }

            dataCacheWriter.finishCurrentSegment();
            List<Segment> pendingSegments = dataCacheWriter.getFinishSegments();
            if (pendingSegments.size() == 0) {
                return;
            }
            DataCacheReader<DenseVector> dataCacheReader =
                    new DataCacheReader<>(
                            DenseVectorSerializer.INSTANCE,
                            basePath.getFileSystem(),
                            pendingSegments);

            while (dataCacheReader.hasNext()) {
                DenseVector point = dataCacheReader.next();
                int closestCentroidId = findClosestCentroidId(centroids, point, distanceMeasure);
                output.collect(new StreamRecord<>(Tuple2.of(closestCentroidId, point)));
            }
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Tuple2<Integer, DenseVector>> out) {
            centroids = null;
            centroidsState.clear();
        }

        @Override
        public void onIterationTerminated(
                Context context, Collector<Tuple2<Integer, DenseVector>> collector)
                throws IOException {
            //            dataCacheWriter.cleanup();
            centroids = null;
            centroidsState.clear();
        }
    }

    protected static int findClosestCentroidId(
            DenseVector[] centroids, DenseVector point, DistanceMeasure distanceMeasure) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        for (int i = 0; i < centroids.length; i++) {
            DenseVector centroid = centroids[i];
            double distance = distanceMeasure.distance(centroid, point);
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = i;
            }
        }
        return closestCentroidId;
    }

    public static DataStream<DenseVector[]> selectRandomCentroids(
            DataStream<DenseVector> data, int k, long seed) {
        DataStream<DenseVector[]> resultStream =
                DataStreamUtils.mapPartition(
                        data,
                        new MapPartitionFunction<DenseVector, DenseVector[]>() {
                            @Override
                            public void mapPartition(
                                    Iterable<DenseVector> iterable, Collector<DenseVector[]> out) {
                                List<DenseVector> vectors = new ArrayList<>();
                                for (DenseVector vector : iterable) {
                                    vectors.add(vector);
                                }
                                Collections.shuffle(vectors, new Random(seed));
                                out.collect(vectors.subList(0, k).toArray(new DenseVector[0]));
                            }
                        });

        resultStream.getTransformation().setParallelism(1);
        return resultStream;
    }
}
