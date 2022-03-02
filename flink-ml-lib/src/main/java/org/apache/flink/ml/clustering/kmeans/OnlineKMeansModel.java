package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData.ModelDataDecoder;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.ml.util.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OnlineKMeansModel implements Model<OnlineKMeansModel>, KMeansModelParams<OnlineKMeansModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private final OutputTag<DenseVector[]> outputTag = new OutputTag<DenseVector[]>("latest-model-data"){};
    private Table initModelDataTable;
//    public DataStream<KMeansModelData> initModelDataStream;
    private Table modelDataTable;
    private DataStream<DenseVector[]> latestModelData;

    public OnlineKMeansModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    public OnlineKMeansModel(Table... initModelDataTables) {
//        this.initModelDataStream = KMeansModelData.getModelDataStream(initModelDataTables[0]);
        Preconditions.checkArgument(initModelDataTables.length == 1);
        this.initModelDataTable = initModelDataTables[0];
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public OnlineKMeansModel setModelData(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        modelDataTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelDataTable};
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkNotNull(initModelDataTable);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        DataStream<KMeansModelData> modelDataStream =
                KMeansModelData.getModelDataStream(modelDataTable);

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol()));

        final String broadcastModelKey = "broadcastModelKey";
        DataStream<Row> predictionResult =
                BroadcastUtils.withBroadcastStream(
                        Arrays.asList(tEnv.toDataStream(inputs[0]), modelDataStream),
                        Collections.singletonMap(broadcastModelKey, KMeansModelData.getModelDataStream(initModelDataTable)),
                        inputList -> {
                            DataStream inputData = inputList.get(0);
                            DataStream modelData = inputList.get(1);
                            DataStream<Row> prediction =
                                    modelData
                                            .broadcast()
                                            .connect(inputData)
                                            .process(new PredictLabelFunction(
                                                            broadcastModelKey,
                                                            getFeaturesCol(),
                                                            DistanceMeasure.getInstance(getDistanceMeasure())),
                                                    outputTypeInfo);
                            return prediction;
                        });

        SingleOutputStreamOperator<Row> operator = new SingleOutputStreamOperator<>(env, predictionResult.getTransformation());
        latestModelData = operator.getSideOutput(outputTag);

        return new Table[] {tEnv.fromDataStream(predictionResult)};
//        return new Table[1];
    }

    @VisibleForTesting
    public DataStream<DenseVector[]> getLatestModelData() {
        return latestModelData;
    }

    /** A utility function used for prediction. */
    private static class PredictLabelFunction extends CoProcessFunction<KMeansModelData, Row, Row> {
        private final OutputTag<DenseVector[]> outputTag = new OutputTag<DenseVector[]>("latest-model-data"){};

        private final String broadcastModelKey;

        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private DenseVector[] centroids = null;

        public PredictLabelFunction(
                String broadcastModelKey, String featuresCol, DistanceMeasure distanceMeasure) {
            this.broadcastModelKey = broadcastModelKey;
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void processElement1(KMeansModelData modelData, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) {
            centroids = modelData.centroids;
            System.out.println("KMeansModel received model data " + Arrays.toString(centroids));
            ctx.output(outputTag, centroids);
        }

        @Override
        public void processElement2(Row dataPoint, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) {
            if (centroids == null) {
                KMeansModelData modelData =
                        (KMeansModelData)
                                getRuntimeContext().getBroadcastVariable(broadcastModelKey).get(0);
                centroids = modelData.centroids;
                System.out.println("KMeansModel received init model data " + Arrays.toString(centroids));
                ctx.output(outputTag, centroids);
            }
            DenseVector point = (DenseVector) dataPoint.getField(featuresCol);
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
            collector.collect(Row.join(dataPoint, Row.of(closestCentroidId)));
        }
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        String modelDataPath = Paths.get(path, "modelData").toString();
        new File(modelDataPath).mkdirs();
        FileSink<KMeansModelData> modelDataSink =
                FileSink.forRowFormat(
                                new org.apache.flink.core.fs.Path(modelDataPath),
                                new KMeansModelData.ModelDataEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
        KMeansModelData.getModelDataStream(modelDataTable).sinkTo(modelDataSink);

        String initModelDataPath = Paths.get(path, "initModelData").toString();
        FileSink<KMeansModelData> initModelDataSink =
                FileSink.forRowFormat(
                                new org.apache.flink.core.fs.Path(initModelDataPath),
                                new KMeansModelData.ModelDataEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
        KMeansModelData.getModelDataStream(initModelDataTable).sinkTo(initModelDataSink);

        ReadWriteUtils.saveMetadata(this, path);
    }

    // TODO: Add INFO level logging.
    public static OnlineKMeansModel load(StreamExecutionEnvironment env, String path) throws IOException {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Source<KMeansModelData, ?, ?> modelDataSource =
                FileSource.forRecordStreamFormat(new ModelDataDecoder(),
                        new org.apache.flink.core.fs.Path(Paths.get(path, "modelData").toString()))
                        .monitorContinuously(Duration.ofMillis(1000))
                        .build();
        DataStream<KMeansModelData> modelDataStream = env.fromSource(modelDataSource, WatermarkStrategy.noWatermarks(), "modelData");

        Source<KMeansModelData, ?, ?> initModelDataSource =
                FileSource.forRecordStreamFormat(new ModelDataDecoder(),
                        new org.apache.flink.core.fs.Path(Paths.get(path, "initModelData").toString())).build();
        DataStream<KMeansModelData> initModelDataStream = env.fromSource(initModelDataSource, WatermarkStrategy.noWatermarks(), "initModelData");

        OnlineKMeansModel model = ReadWriteUtils.loadStageParam(path);
        model.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
        return model.setModelData(tEnv.fromDataStream(modelDataStream));
    }

    public static org.apache.flink.core.fs.Path[] getDataPaths(String path) {
//        String dataPath = Paths.get(path, "modelData").toString();
        File[] files = new File(path).listFiles();

        org.apache.flink.core.fs.Path[] paths = new org.apache.flink.core.fs.Path[files.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = org.apache.flink.core.fs.Path.fromLocalFile(files[i]);
        }

        return paths;
    }
}
