package org.apache.flink.ml.tree;

import org.apache.flink.ml.linalg.Vector;

public class LeafNode implements Node {
    public double prediction;

    public LeafNode(double prediction) {
        this.prediction = prediction;
    }

    @Override
    public LeafNode predict(Vector features) {
        return this;
    }
}
