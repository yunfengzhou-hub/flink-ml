package org.apache.flink.ml.tree;


import org.apache.flink.ml.linalg.Vector;

public interface Split {
    boolean shouldGoLeft(Vector features);
}
