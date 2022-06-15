/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.linalg.typeinfo;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.DenseVectorWithNorm;

import java.io.IOException;
import java.util.Arrays;

/** Specialized serializer for {@link DenseVectorWithNorm}. */
public class DenseVectorWithNormSerializer extends TypeSerializer<DenseVectorWithNorm> {
    private final DenseVectorSerializer denseVectorSerializer = new DenseVectorSerializer();

    private static final long serialVersionUID = 1L;

    private static final double[] EMPTY = new double[0];

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<DenseVectorWithNorm> duplicate() {
        return new DenseVectorWithNormSerializer();
    }

    @Override
    public DenseVectorWithNorm createInstance() {
        return new DenseVectorWithNorm(new DenseVector(EMPTY));
    }

    @Override
    public DenseVectorWithNorm copy(DenseVectorWithNorm from) {
        DenseVector newDenseVector =
                new DenseVector(Arrays.copyOf(from.values, from.values.length));
        return new DenseVectorWithNorm(newDenseVector);
    }

    @Override
    public DenseVectorWithNorm copy(DenseVectorWithNorm from, DenseVectorWithNorm reuse) {
        if (from.values.length == reuse.values.length) {
            System.arraycopy(from.values, 0, reuse.values, 0, from.values.length);
            reuse.l2Norm = from.l2Norm;
            return reuse;
        }

        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DenseVectorWithNorm from, DataOutputView dataOutputView)
            throws IOException {
        denseVectorSerializer.serialize(from, dataOutputView);
        dataOutputView.writeDouble(from.l2Norm);
    }

    @Override
    public DenseVectorWithNorm deserialize(DataInputView dataInputView) throws IOException {
        DenseVector vector = denseVectorSerializer.deserialize(dataInputView);
        double l2NormSquare = dataInputView.readDouble();
        return new DenseVectorWithNorm(vector.values, l2NormSquare);
    }

    @Override
    public DenseVectorWithNorm deserialize(DenseVectorWithNorm reuse, DataInputView dataInputView)
            throws IOException {
        DenseVector vector = denseVectorSerializer.deserialize(reuse, dataInputView);
        if (vector == reuse) {
            reuse.l2Norm = dataInputView.readDouble();
            return reuse;
        }

        double l2NormSquare = dataInputView.readDouble();
        return new DenseVectorWithNorm(vector.values, l2NormSquare);
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView)
            throws IOException {
        denseVectorSerializer.copy(dataInputView, dataOutputView);
        dataOutputView.write(dataInputView, 8);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DenseVectorWithNormSerializer;
    }

    @Override
    public int hashCode() {
        return DenseVectorWithNormSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<DenseVectorWithNorm> snapshotConfiguration() {
        return new DenseVectorWithNormSerializerSnapshot();
    }

    private static class DenseVectorWithNormSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DenseVectorWithNorm> {
        public DenseVectorWithNormSerializerSnapshot() {
            super(DenseVectorWithNormSerializer::new);
        }
    }
}
