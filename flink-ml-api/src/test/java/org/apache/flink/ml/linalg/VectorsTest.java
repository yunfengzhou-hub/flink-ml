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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests the behavior of Vectors. */
public class VectorsTest {
    int n;
    int[] indices;
    double[] values;

    @Before
    public void before() {
        n = 4;
        indices = new int[] {0, 2, 3};
        values = new double[] {0.1, 0.3, 0.4};
    }

    @Test
    public void testSparseVector() {
        SparseVector vector = Vectors.sparse(n, indices, values);
        assertEquals(n, vector.n);
        assertArrayEquals(indices, vector.indices);
        assertArrayEquals(values, vector.values, 1e-5);
    }
}
