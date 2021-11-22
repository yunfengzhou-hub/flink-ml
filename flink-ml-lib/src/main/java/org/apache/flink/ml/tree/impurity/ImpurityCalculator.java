package org.apache.flink.ml.tree.impurity;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public abstract class ImpurityCalculator implements Serializable {
    protected final double[] stats;
    protected long rawCount;

    protected ImpurityCalculator(double[] stats) {
        this.stats = stats;
    }

    abstract double calculate();

    ImpurityCalculator add(ImpurityCalculator other) {
        Preconditions.checkArgument(stats.length == other.stats.length,
                String.format("Two ImpurityCalculator instances cannot be added with different counts sizes." +
                        "  Sizes are {} and {}.", stats.length, other.stats.length));
        for (int i = 0; i < other.stats.length; i++) {
            stats[i] += other.stats[i];
        }
        rawCount += other.rawCount;
        return this;
    }

    ImpurityCalculator subtract(ImpurityCalculator other) {
        Preconditions.checkArgument(stats.length == other.stats.length,
                String.format("Two ImpurityCalculator instances cannot be subtracted with different counts sizes." +
                        "  Sizes are {} and {}.", stats.length, other.stats.length));
        for (int i = 0; i < other.stats.length; i++) {
            stats[i] -= other.stats[i];
        }
        rawCount -= other.rawCount;
        return this;
    }
}
