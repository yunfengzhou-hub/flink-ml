package org.apache.flink.ml.regression.decisiontree;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.iteration.config.IterationOptions;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DecisionTreeTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

//    private DataTypes.Field[] inputType;
    Schema schema;
    private Row[] trainData;
//    private Row[] predictData;
//    private Row[] expectedOutput;
    String errorMessage;


    @Before
    public void Setup() throws IOException {
        env = StreamExecutionEnvironment.createLocalEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 18082);
        configuration.set(
                IterationOptions.DATA_CACHE_PATH,
                "file://" + tempFolder.newFolder().getAbsolutePath());
        configuration.set(
                ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.getConfig().setGlobalJobParameters(configuration);

        schema = Schema.newBuilder()
                .column("features", DataTypes.of(DenseVector.class))
                .column("label", DataTypes.STRING())
                .build();

        trainData = new Row[]{
                Row.of(new DenseVector(new double[]{1.0, 1.0}), "l0"),
                Row.of(new DenseVector(new double[]{2.0, 1.0}), "l0"),
                Row.of(new DenseVector(new double[]{1.0, 2.0}), "l1"),
                Row.of(new DenseVector(new double[]{2.0, 2.0}), "l2"),
        };
    }

    @Test
    public void testDecisionTree() {
        errorMessage = "Normal test for decision tree";
        runAndCheck();
    }

    private void runAndCheck() {
        DataStream<Row> trainStream = env.fromElements(trainData);
        Table trainTable = tEnv.fromDataStream(trainStream, schema);

        DecisionTree estimator = new DecisionTree();

        DecisionTreeModel model = estimator.fit(trainTable);

        Table actualOutputTable = model.transform(trainTable)[0];

        Object[] actualObjects = IteratorUtils.toArray(actualOutputTable.execute().collect());
//        Row[] actual = new Row[actualObjects.length];
        for (int i=0; i<actualObjects.length;i++) {
            System.out.println(actualObjects[i]);
//            actual[i] = (Row) actualObjects[i];
        }

//        Assert.assertEquals(errorMessage, getFrequencyMap(expectedOutput), getFrequencyMap(actual));
    }

    private static Map<Object, Integer> getFrequencyMap(Row[] rows) {
        Map<Object, Integer> map = new HashMap<>();
        for (Row row: rows) {
            List<Object> list = toList(row);
            map.put(list, map.getOrDefault(list, 0) + 1);
        }
        return map;
    }

    private static List<Object> toList(Row row) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            list.add(row.getField(i));
        }
        return list;
    }
}
