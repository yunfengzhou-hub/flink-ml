package org.apache.flink.ml.clustering;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeans;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.MockBlockingQueueSourceFunction;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

public class OnlineKmeansTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private static final DenseVectorSerializer serializer = new DenseVectorSerializer();
    private String trainId;
    private String predictId;
    private static final List<DenseVector> DATA =
            Arrays.asList(
                    Vectors.dense(0.0, 0.0),
                    Vectors.dense(0.0, 0.3),
                    Vectors.dense(0.3, 0.0),
                    Vectors.dense(9.0, 0.0),
                    Vectors.dense(9.0, 0.6),
                    Vectors.dense(9.6, 0.0));
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private static final List<Set<DenseVector>> expectedGroups =
            Arrays.asList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(0.0, 0.0),
                                    Vectors.dense(0.0, 0.3),
                                    Vectors.dense(0.3, 0.0))),
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(9.0, 0.0),
                                    Vectors.dense(9.0, 0.6),
                                    Vectors.dense(9.6, 0.0))));
    private Table trainTable;
    private Table predictTable;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        trainId = String.valueOf(System.currentTimeMillis());
        predictId = String.valueOf(System.currentTimeMillis() + 1);
        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();
        trainTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockBlockingQueueSourceFunction<>(
                                                trainId, new DenseVectorDeserializer()),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
        predictTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockBlockingQueueSourceFunction<>(
                                                predictId, new DenseVectorDeserializer()),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
    }

    @Test
    public void testFeaturePredictionParam() throws Exception {
        OnlineKMeans kmeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        KMeansModel model = kmeans.fit(trainTable);
        Table output = model.transform(predictTable)[0];

        pushVectors(
                trainId,
                Vectors.dense(0.0, 0.0),
                Vectors.dense(0.0, 0.3),
                Vectors.dense(0.3, 0.0),
                Vectors.dense(9.0, 0.0),
                Vectors.dense(9.0, 0.6),
                Vectors.dense(9.6, 0.0));

//        System.out.println(model.getModelData()[0].execute().collect().next());

        pushVectors(
                predictId,
                Vectors.dense(-1.0, -1.0)
        );

        Iterator<Row> iterator = output.execute().collect();
        System.out.println(iterator.next().getField("prediction"));

        Thread.sleep(5000);

        pushVectors(
                predictId,
                Vectors.dense(0.0, 0.0),
                Vectors.dense(0.0, -5.0)
        );

        Row row1 = iterator.next();
        Row row2 = iterator.next();
        System.out.println(row1 + " " + row2);

        pushVectors(
                trainId,
                Vectors.dense(0.0, 105.0),
                Vectors.dense(0.0, 105.3),
                Vectors.dense(0.3, 105.0),
                Vectors.dense(9.0, -105.0),
                Vectors.dense(9.0, -105.6),
                Vectors.dense(9.6, -105.0));

        Thread.sleep(1000);

        pushVectors(
                predictId,
                Vectors.dense(1.0, -5.0),
                Vectors.dense(1.0, 5.0),
                Vectors.dense(1.0, -5.0),
                Vectors.dense(1.0, 5.0)
        );

        row1 = iterator.next();
        row2 = iterator.next();
        System.out.println(row1 + " " + row2);
        row1 = iterator.next();
        row2 = iterator.next();
        System.out.println(row1 + " " + row2);



    }

    private static Row getNextStableData(Iterator<Row> iterator) {
        for (int i = 0; i < 10; i++) {
            iterator.next();
        }
        return iterator.next();
    }

    private static void pushVectors(String id, DenseVector... vectors) {
        List<String> vectorStrings = new ArrayList<>();
        for (DenseVector vector : vectors) {
            vectorStrings.add(serializer.apply(vector));
        }
        MockBlockingQueueSourceFunction.getQueue(id).addAll(vectorStrings);
    }

    private static class DenseVectorSerializer implements Function<DenseVector, String> {
        @Override
        public String apply(DenseVector denseVector) {
            StringBuilder str = new StringBuilder(String.valueOf(denseVector.values[0]));
            for (int i = 1; i < denseVector.size(); i++) {
                str.append(" ").append(denseVector.get(i));
            }
            return str.toString();
        }
    }

    private static class DenseVectorDeserializer
            implements Function<String, DenseVector>, Serializable {
        @Override
        public DenseVector apply(String s) {
            String[] strings = s.split(" ");
            DenseVector vector = new DenseVector(strings.length);
            for (int i = 0; i < strings.length; i++) {
                vector.values[i] = Double.parseDouble(strings[i]);
            }
            return vector;
        }
    }
}
