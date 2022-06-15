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
import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.VectorWithNorm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/** Specialized serializer for {@link VectorWithNorm}. */
public class VectorWithNormSerializer extends TypeSerializer<VectorWithNorm> {
    private final DenseVectorSerializer denseVectorSerializer = new DenseVectorSerializer();

    private static final SparseVectorSerializer sparseVectorSerializer =
            SparseVectorSerializer.INSTANCE;

    private static final long serialVersionUID = 1L;

    private static final double[] EMPTY = new double[0];

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<VectorWithNorm> duplicate() {
        return new VectorWithNormSerializer();
    }

    @Override
    public VectorWithNorm createInstance() {
        return new VectorWithNorm(new DenseVector(EMPTY));
    }

    @Override
    public VectorWithNorm copy(VectorWithNorm from) {
        Vector vector;
        if (from.getVector() instanceof DenseVector) {
            vector = denseVectorSerializer.copy((DenseVector) from.getVector());
        } else {
            vector = sparseVectorSerializer.copy((SparseVector) from.getVector());
        }

        return new VectorWithNorm(vector, from.getL2Norm());
    }

    @Override
    public VectorWithNorm copy(VectorWithNorm from, VectorWithNorm reuse) {
        Vector vector;
        if (from.getVector() instanceof DenseVector && reuse.getVector() instanceof DenseVector) {
            vector =
                    denseVectorSerializer.copy(
                            (DenseVector) from.getVector(), (DenseVector) reuse.getVector());
        } else if (from.getVector() instanceof SparseVector
                && reuse.getVector() instanceof SparseVector) {
            vector =
                    sparseVectorSerializer.copy(
                            (SparseVector) from.getVector(), (SparseVector) reuse.getVector());
        } else {
            vector = from.getVector().clone();
        }

        if (vector == reuse.getVector()) {
            reuse.l2Norm = from.l2Norm;
            return reuse;
        }

        return new VectorWithNorm(vector, from.l2Norm);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(VectorWithNorm from, DataOutputView dataOutputView) throws IOException {
        dataOutputView.writeByte(from.getVector() instanceof SparseVector ? 1 : 0);
        if (from.getVector() instanceof DenseVector) {
            denseVectorSerializer.serialize((DenseVector) from.getVector(), dataOutputView);
        } else {
            sparseVectorSerializer.serialize((SparseVector) from.getVector(), dataOutputView);
        }

        dataOutputView.writeDouble(from.l2Norm);
    }

    @Override
    public VectorWithNorm deserialize(DataInputView dataInputView) throws IOException {
        byte vectorType = dataInputView.readByte();
        Vector vector = deserializeVector(vectorType, dataInputView);
        double l2NormSquare = dataInputView.readDouble();
        return new VectorWithNorm(vector, l2NormSquare);
    }

    @Override
    public VectorWithNorm deserialize(VectorWithNorm reuse, DataInputView dataInputView)
            throws IOException {
        byte vectorType = dataInputView.readByte();

        Vector vector;
        if (vectorType == 0 && reuse.getVector() instanceof DenseVector) {
            vector =
                    denseVectorSerializer.deserialize(
                            (DenseVector) reuse.getVector(), dataInputView);
        } else if (vectorType == 1 && reuse.getVector() instanceof SparseVector) {
            vector =
                    sparseVectorSerializer.deserialize(
                            (SparseVector) reuse.getVector(), dataInputView);
        } else {
            vector = deserializeVector(vectorType, dataInputView);
        }

        if (vector == reuse.getVector()) {
            reuse.l2Norm = dataInputView.readDouble();
            return reuse;
        }

        double l2NormSquare = dataInputView.readDouble();
        return new VectorWithNorm(vector, l2NormSquare);
    }

    private Vector deserializeVector(byte vectorType, DataInputView dataInputView)
            throws IOException {
        switch (vectorType) {
            case 0:
                return denseVectorSerializer.deserialize(dataInputView);
            case 1:
                return sparseVectorSerializer.deserialize(dataInputView);
            default:
                throw new UnsupportedEncodingException();
        }
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView)
            throws IOException {
        denseVectorSerializer.copy(dataInputView, dataOutputView);
        dataOutputView.write(dataInputView, 8);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VectorWithNormSerializer;
    }

    @Override
    public int hashCode() {
        return VectorWithNormSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<VectorWithNorm> snapshotConfiguration() {
        return new DenseVectorWithNormSerializerSnapshot();
    }

    private static class DenseVectorWithNormSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<VectorWithNorm> {
        public DenseVectorWithNormSerializerSnapshot() {
            super(VectorWithNormSerializer::new);
        }
    }
}
