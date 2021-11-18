package org.apache.flink.ml.linalg;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VectorsTest {
    int n;
    int[] indices;
    double[] values;

    @Before
    public void before() {
        n = 4;
        indices = new int[]{0, 2, 3};
        values = new double[]{0.1, 0.3, 0.4};
    }

    @Test
    public void testSparseVector() {
        SparseVector vector = Vectors.sparse(n, indices, values);
        assertEquals(n, vector.n);
        assertArrayEquals(indices, vector.indices);
        assertArrayEquals(values, vector.values, 1e-5);
    }
}
