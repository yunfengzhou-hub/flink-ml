package org.apache.flink.ml.tree;

import org.apache.flink.ml.linalg.Vector;

import java.io.Serializable;

public interface Node extends Serializable {
    LeafNode predict(Vector features);
}
