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

package org.apache.flink.ml.feature.doublearray;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Transformer;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** VectorToDoubleArray. */
public class VectorToDoubleArray
        implements Transformer<VectorToDoubleArray>,
                DoubleArrayToVectorParams<VectorToDoubleArray> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public VectorToDoubleArray() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        TypeInformation<?>[] outputTypes = inputTypeInfo.getFieldTypes();
        String[] outputFieldNames = inputTypeInfo.getFieldNames();
        for (int i = 0; i < outputFieldNames.length; i++) {
            if (Arrays.asList(getInputCols()).contains(outputFieldNames[i])) {
                outputTypes[i] = Types.PRIMITIVE_ARRAY(Types.DOUBLE);
            }
        }

        RowTypeInfo outputTypeInfo = new RowTypeInfo(outputTypes, outputFieldNames);

        DataStream<Row> stream =
                tEnv.toDataStream(inputs[0])
                        .map(new VectorToDoubleArrayFunction(getInputCols()), outputTypeInfo);

        return new Table[] {tEnv.fromDataStream(stream)};
    }

    private static class VectorToDoubleArrayFunction implements MapFunction<Row, Row> {
        private final String[] inputCols;

        private VectorToDoubleArrayFunction(String[] inputCols) {
            this.inputCols = inputCols;
        }

        @Override
        public Row map(Row value) throws Exception {
            for (int i = 0; i < inputCols.length; i++) {
                Vector vector = value.getFieldAs(inputCols[i]);
                value.setField(i, vector.toArray());
            }
            return value;
        }
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static VectorToDoubleArray load(StreamTableEnvironment tEnv, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
