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

package org.apache.flink.ml.feature.onehotencoder;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;

import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/** Provides classes to save/load model data. */
public class OneHotEncoderModelData {
    public static Table fromDataStream(
            StreamTableEnvironment tEnv, DataStream<Tuple2<Integer, Integer>> stream) {
        return tEnv.fromDataStream(stream);
    }

    public static DataStream<Tuple2<Integer, Integer>> toDataStream(
            StreamTableEnvironment tEnv, Table table) {
        return tEnv.toDataStream(table)
                .map(
                        new MapFunction<Row, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Row row) throws Exception {
                                return new Tuple2<>(
                                        (int) row.getField("f0"), (int) row.getField("f1"));
                            }
                        });
    }

    /** Encoder for the OneHotEncoder model data. */
    public static class ModelDataEncoder implements Encoder<Tuple2<Integer, Integer>> {
        @Override
        public void encode(Tuple2<Integer, Integer> modeldata, OutputStream outputStream)
                throws IOException {
            Output output = new Output(outputStream);
            String modelDataString = ReadWriteUtils.OBJECT_MAPPER.writeValueAsString(modeldata);
            output.write(modelDataString.getBytes());
            output.flush();
        }
    }

    /** Decoder for the OneHotEncoder model data. */
    public static class ModelDataStreamFormat extends SimpleStreamFormat<Tuple2<Integer, Integer>> {
        @Override
        public Reader<Tuple2<Integer, Integer>> createReader(
                Configuration config, FSDataInputStream stream) throws IOException {
            return new Reader<Tuple2<Integer, Integer>>() {
                private final MappingIterator<Map> iterator =
                        ReadWriteUtils.OBJECT_MAPPER.readValues(
                                new JsonFactory().createParser(stream), Map.class);

                @Override
                public Tuple2<Integer, Integer> read() throws IOException {
                    if (!iterator.hasNext()) {
                        return null;
                    }
                    Map<String, ?> map = iterator.next();
                    return new Tuple2<>((Integer) map.get("f0"), (Integer) map.get("f1"));
                }

                @Override
                public void close() throws IOException {
                    stream.close();
                }
            };
        }

        @Override
        public TypeInformation<Tuple2<Integer, Integer>> getProducedType() {
            return Types.TUPLE(Types.INT, Types.INT);
        }
    }
}
