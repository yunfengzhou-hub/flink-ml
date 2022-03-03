package org.apache.flink.ml.clustering.kmeans;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData.ModelDataDecoder;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData.ModelDataEncoder;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamingKMeansModel implements Model<StreamingKMeansModel>, KMeansModelParams<StreamingKMeansModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private static final OutputTag<DenseVector[]> outputTag = new OutputTag<DenseVector[]>("latest-model-data"){};
    private Table initModelDataTable;
    private Table modelDataTable;
    private DataStream<DenseVector[]> latestModelData;

    public StreamingKMeansModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    public StreamingKMeansModel(Table... initModelDataTables) {
        Preconditions.checkArgument(initModelDataTables.length == 1);
        this.initModelDataTable = initModelDataTables[0];
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public StreamingKMeansModel setModelData(Table... inputs) {
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

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol()));

        DataStream<Row> predictionResult =
                BroadcastUtils.withBroadcastStream(
                        Arrays.asList(tEnv.toDataStream(inputs[0]), KMeansModelData.getModelDataStream(modelDataTable)),
                        Collections.singletonMap(KMeansModel.broadcastModelKey, KMeansModelData.getModelDataStream(initModelDataTable)),
                        inputList -> {
                            DataStream inputData = inputList.get(0);
                            DataStream modelData = inputList.get(1);
                            DataStream<Row> prediction =
                                    modelData
                                            .broadcast()
                                            .connect(inputData)
                                            .process(new PredictLabelFunction(
                                                            getFeaturesCol(),
                                                            DistanceMeasure.getInstance(getDistanceMeasure())),
                                                    outputTypeInfo);
                            return prediction;
                        });

        latestModelData = DataStreamUtils.getSideOutput(predictionResult, outputTag);

        return new Table[] {tEnv.fromDataStream(predictionResult)};
    }

    @VisibleForTesting
    public DataStream<DenseVector[]> getLatestModelData() {
        return latestModelData;
    }

    /** A utility function used for prediction. */
    private static class PredictLabelFunction extends CoProcessFunction<KMeansModelData, Row, Row> {
        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private DenseVector[] centroids;

        public PredictLabelFunction(
                String featuresCol, DistanceMeasure distanceMeasure) {
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void processElement1(KMeansModelData modelData, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) {
            centroids = modelData.centroids;
            ctx.output(outputTag, centroids);
        }

        @Override
        public void processElement2(Row dataPoint, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) {
            if (centroids == null) {
                KMeansModelData modelData =
                        (KMeansModelData)
                                getRuntimeContext().getBroadcastVariable(KMeansModel.broadcastModelKey).get(0);
                processElement1(modelData, ctx, collector);
            }
            DenseVector point = (DenseVector) dataPoint.getField(featuresCol);
            int closestCentroidId = KMeans.findClosestCentroidId(centroids, point, distanceMeasure);
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
        ReadWriteUtils.saveDataStream(
                KMeansModelData.getModelDataStream(modelDataTable),
                modelDataPath,
                new ModelDataEncoder());

        String initModelDataPath = Paths.get(path, "initModelData").toString();
        ReadWriteUtils.saveDataStream(
                KMeansModelData.getModelDataStream(initModelDataTable),
                initModelDataPath,
                new ModelDataEncoder());

        ReadWriteUtils.saveMetadata(this, path);
    }

    // TODO: Add INFO level logging.
    public static StreamingKMeansModel load(StreamExecutionEnvironment env, String path) throws IOException {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<KMeansModelData> modelDataStream = ReadWriteUtils.loadUnboundedStream(
                env, Paths.get(path, "modelData").toString(), new ModelDataDecoder()
        );

        DataStream<KMeansModelData> initModelDataStream = ReadWriteUtils.loadBoundedStream(
                env, Paths.get(path, "initModelData").toString(), new ModelDataDecoder()
        );

        StreamingKMeansModel model = ReadWriteUtils.loadStageParam(path);
        model.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
        return model.setModelData(tEnv.fromDataStream(modelDataStream));
    }
}
