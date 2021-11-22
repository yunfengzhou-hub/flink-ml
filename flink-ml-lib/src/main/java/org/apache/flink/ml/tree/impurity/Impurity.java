package org.apache.flink.ml.tree.impurity;

import java.io.Serializable;

public interface Impurity extends Serializable {
    double calculate(double[] counts, double totalCount);

    double calculate(double count, double sum, double sumSquares);
}

