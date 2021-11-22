package org.apache.flink.ml.tree.impurity;

import java.io.Serializable;

public abstract class ImpurityAggregator implements Serializable {
    final int statsSize;

    protected ImpurityAggregator(int statsSize) {
        this.statsSize = statsSize;
    }

    void merge(double[] allStats, int offset, int otherOffset) {
        for (int i = 0; i < statsSize; i++) {
            allStats[offset + i] += allStats[otherOffset + i];
        }
    }

    abstract void update(double[] allStats, int offset, double label, int numSamples, double sampleWeight);

    abstract ImpurityCalculator getCalculator(double[] array, int offset);
}
