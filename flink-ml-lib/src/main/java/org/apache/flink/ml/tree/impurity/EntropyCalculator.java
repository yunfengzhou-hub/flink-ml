package org.apache.flink.ml.tree.impurity;

public class EntropyCalculator extends ImpurityCalculator{
    protected EntropyCalculator(double[] stats, long rawCount) {
        super(stats);
        this.rawCount = rawCount;
    }

    @Override
    double calculate() {
        return new Entropy().calculate(stats, stats.length);
    }
}
