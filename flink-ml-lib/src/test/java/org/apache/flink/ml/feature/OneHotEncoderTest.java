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

package org.apache.flink.ml.feature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModelData;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests OneHotEncoder and OneHotEncoderModel. */
public class OneHotEncoderTest {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Schema schema;
    private Row[] trainData;
    private Row[] predictData;
    private Row[] expectedOutput;
    private String[] inputCols;
    private String[] outputCols;
    private boolean dropLast;
    private boolean isSaveLoad;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.DOUBLE())
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build();

        trainData =
                new Row[] {
                    Row.of(0.0), Row.of(1.0), Row.of(2.0), Row.of(0.0),
                };

        expectedOutput =
                new Row[] {
                    Row.of(Vectors.sparse(2, 0, 1.0)),
                    Row.of(Vectors.sparse(2, 1, 1.0)),
                    Row.of(Vectors.sparse(2)),
                    Row.of(Vectors.sparse(2, 0, 1.0)),
                };

        predictData = trainData;
        inputCols = new String[] {"f0"};
        outputCols = new String[] {"output_f0"};
        dropLast = true;
        isSaveLoad = false;
    }

    @Test
    public void testParam() {
        OneHotEncoder estimator = new OneHotEncoder();

        assertTrue(estimator.getDropLast());

        estimator.setInputCols("test_input").setOutputCols("test_output").setDropLast(false);

        assertArrayEquals(new String[] {"test_input"}, estimator.getInputCols());
        assertArrayEquals(new String[] {"test_output"}, estimator.getOutputCols());
        assertFalse(estimator.getDropLast());

        OneHotEncoderModel model = new OneHotEncoderModel();

        assertTrue(model.getDropLast());

        model.setInputCols("test_input").setOutputCols("test_output").setDropLast(false);

        assertArrayEquals(new String[] {"test_input"}, model.getInputCols());
        assertArrayEquals(new String[] {"test_output"}, model.getOutputCols());
        assertFalse(model.getDropLast());
    }

    @Test
    public void testOneHotEncoder() throws Exception {
        runAndCheck();
    }

    @Test
    public void testDropLast() throws Exception {
        dropLast = !dropLast;

        expectedOutput =
                new Row[] {
                    Row.of(Vectors.sparse(3, 0, 1.0)),
                    Row.of(Vectors.sparse(3, 1, 1.0)),
                    Row.of(Vectors.sparse(3, 2, 1.0)),
                    Row.of(Vectors.sparse(3, 0, 1.0)),
                };
        runAndCheck();
    }

    @Test
    public void testInputDataType() throws Exception {
        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build();

        trainData =
                new Row[] {
                    Row.of(0), Row.of(1), Row.of(2), Row.of(0),
                };
        predictData = trainData;
        runAndCheck();
    }

    @Test(expected = Exception.class)
    public void testNonIntegerDouble() throws Exception {
        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.DOUBLE())
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build();

        trainData =
                new Row[] {
                    Row.of(0.5), Row.of(1.5), Row.of(2.5), Row.of(0.5),
                };
        runAndCheck();
    }

    @Test(expected = Exception.class)
    public void testNonIntegerDouble2() throws Exception {
        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.DOUBLE())
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build();

        predictData =
                new Row[] {
                    Row.of(0.5), Row.of(1.5), Row.of(2.5), Row.of(0.5),
                };
        runAndCheck();
    }

    @Test
    public void testSaveLoad() throws Exception {
        isSaveLoad = true;
        runAndCheck();
    }

    @Test
    public void testGetModelData() throws Exception {
        OneHotEncoderModel model = getModel();
        Tuple2<Integer, Integer> expected = new Tuple2<>(0, 2);
        Tuple2<Integer, Integer> actual =
                OneHotEncoderModelData.toDataStream(tEnv, model.getModelData()[0])
                        .executeAndCollect()
                        .next();
        assertEquals(expected, actual);
    }

    @Test
    public void testSetModelData() throws Exception {
        OneHotEncoderModel modelA = getModel();

        Table modelData = modelA.getModelData()[0];
        OneHotEncoderModel modelB = new OneHotEncoderModel().setModelData(modelData);
        ReadWriteUtils.updateExistingParams(modelB, modelA.getParamMap());

        checkResult(modelB);
    }

    private void runAndCheck() throws Exception {
        OneHotEncoderModel model = getModel();
        checkResult(model);
    }

    private OneHotEncoderModel getModel() throws Exception {
        Table trainTable =
                tEnv.fromDataStream(
                        env.fromElements(trainData)
                                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks()),
                        schema);

        OneHotEncoder estimator =
                new OneHotEncoder()
                        .setDropLast(dropLast)
                        .setInputCols(inputCols)
                        .setOutputCols(outputCols);

        if (isSaveLoad) {
            String tempDir = Files.createTempDirectory("").toString();
            estimator.save(tempDir);
            env.execute();

            estimator = OneHotEncoder.load(env, tempDir);
        }

        OneHotEncoderModel model = estimator.fit(trainTable);

        if (isSaveLoad) {
            String tempDir = Files.createTempDirectory("").toString();
            model.save(tempDir);
            env.execute();

            model = OneHotEncoderModel.load(env, tempDir);
        }

        return model;
    }

    private void checkResult(OneHotEncoderModel model) {
        Table predictTable =
                tEnv.fromDataStream(
                        env.fromElements(predictData)
                                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks()),
                        schema);
        Table output = model.transform(predictTable)[0].select($("output_f0"));

        Object[] actualObjects = IteratorUtils.toArray(output.execute().collect());
        Row[] actual = new Row[actualObjects.length];
        for (int i = 0; i < actualObjects.length; i++) {
            actual[i] = (Row) actualObjects[i];
        }

        assertEquals(getFrequencyMap(expectedOutput), getFrequencyMap(actual));
    }

    private static Map<Object, Integer> getFrequencyMap(Row[] rows) {
        Map<Object, Integer> map = new HashMap<>();
        for (Row row : rows) {
            List<Object> list = getFieldValues(row);
            map.put(list, map.getOrDefault(list, 0) + 1);
        }
        return map;
    }

    private static List<Object> getFieldValues(Row row) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            list.add(row.getField(i));
        }
        return list;
    }
}
