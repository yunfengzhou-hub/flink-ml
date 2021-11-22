package org.apache.flink.ml.regression.decisiontree;

import org.apache.flink.ml.tree.Node;

import java.io.Serializable;

public class DecisionTreeModelData implements Serializable {
    public final Node root;

    public DecisionTreeModelData(Node root) {
        this.root = root;
    }
}
