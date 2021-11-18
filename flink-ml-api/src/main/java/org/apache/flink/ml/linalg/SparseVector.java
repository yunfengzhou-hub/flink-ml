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

package org.apache.flink.ml.linalg;

import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;

/** A sparse vector of double values. */
public class SparseVector implements Vector {
    public final int n;
    public final int[] indices;
    public final double[] values;

    public SparseVector() {
        this(-1);
    }

    public SparseVector(int n) {
        this(n, new int[0], new double[0]);
    }

    public SparseVector(int n, int index, double value) {
        this(n, new int[] {index}, new double[] {value});
    }

    public SparseVector(int n, int[] indices, double[] values) {
        this.n = n;
        this.indices = indices;
        this.values = values;
        checkSizeAndIndicesRange();
        sortIndices();
    }

    @Override
    public int size() {
        return n;
    }

    @Override
    public double get(int i) {
        int pos = Arrays.binarySearch(indices, i);
        if (pos >= 0) {
            return values[pos];
        }
        return 0.;
    }

    @Override
    public double[] toArray() {
        double[] result = new double[n];
        for (int i = 0; i < indices.length; i++) {
            result[indices[i]] = values[i];
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SparseVector that = (SparseVector) o;
        return n == that.n
                && Arrays.equals(indices, that.indices)
                && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(n);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    /**
     * Check whether the indices array and values array are of the same size, and whether vector
     * indices are in valid range.
     */
    private void checkSizeAndIndicesRange() {
        Preconditions.checkArgument(
                indices.length == values.length,
                "Indices size and values size should be the same.");
        for (int index : indices) {
            Preconditions.checkArgument(
                    !(index < 0 || (n >= 0 && index >= n)), "Index out of bound.");
        }
    }

    /** Sort the indices and values if the indices are out of order. */
    private void sortIndices() {
        boolean outOfOrder = false;
        for (int i = 0; i < this.indices.length - 1; i++) {
            if (this.indices[i] > this.indices[i + 1]) {
                outOfOrder = true;
                break;
            }
        }
        if (outOfOrder) {
            sortImpl(this.indices, this.values, 0, this.indices.length - 1);
        }
    }

    /** Sort the indices and values using quick sort. */
    private static void sortImpl(int[] indices, double[] values, int low, int high) {
        int pivotPos = (low + high) / 2;
        int pivot = indices[pivotPos];
        indices[pivotPos] = indices[high];
        indices[high] = pivot;
        double t = values[pivotPos];
        values[pivotPos] = values[high];
        values[high] = t;

        int pos = low - 1;
        for (int i = low; i <= high; i++) {
            if (indices[i] <= pivot) {
                pos++;
                int tempI = indices[pos];
                double tempD = values[pos];
                indices[pos] = indices[i];
                values[pos] = values[i];
                indices[i] = tempI;
                values[i] = tempD;
            }
        }
        if (high > pos + 1) {
            sortImpl(indices, values, pos + 1, high);
        }
        while (pos - 1 > low && indices[pos - 1] == pivot) {
            pos--;
        }
        if (pos - 1 > low) {
            sortImpl(indices, values, low, pos - 1);
        }
    }
}
