package org.apache.flink.ml.tree;

import org.apache.flink.ml.linalg.Vector;

public class InternalNode implements Node {
    private Node leftChild;
    private Node rightChild;
    private Split split;

    public InternalNode(Node leftChild, Node rightChild, Split split) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.split = split;
    }

    @Override
    public LeafNode predict(Vector features) {
        Node node = this;
        while (!(node instanceof LeafNode)) {
            InternalNode n = (InternalNode) node;
            if (n.split.shouldGoLeft(features)) {
                node = n.leftChild;
            } else {
                node = n.rightChild;
            }
        }
        return (LeafNode) node;
    }
}
