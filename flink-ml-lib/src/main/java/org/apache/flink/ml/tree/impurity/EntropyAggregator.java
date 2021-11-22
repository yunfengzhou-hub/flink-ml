package org.apache.flink.ml.tree.impurity;

import org.apache.flink.util.Preconditions;

import java.util.Arrays;

public class EntropyAggregator extends ImpurityAggregator {
    private final int numClasses;
    protected EntropyAggregator(int numClasses) {
        super(numClasses + 1);
        this.numClasses = numClasses;
    }

    @Override
    void update(double[] allStats, int offset, double label, int numSamples, double sampleWeight) {
        Preconditions.checkArgument(label < numClasses, String.format("EntropyAggregator given label {}" +
                " but requires label < statsSize (= {}).", label, numClasses));
        Preconditions.checkArgument(label >= 0, String.format("EntropyAggregator given label {}" +
                "but requires label is non-negative.", label));
        allStats[offset + (int) label] += numSamples * sampleWeight;
        allStats[offset + statsSize - 1] += numSamples;
    }

    @Override
    EntropyCalculator getCalculator(double[] allStats, int offset) {
        return new EntropyCalculator(
                Arrays.copyOfRange(allStats, offset, offset + statsSize - 1),
                (long) allStats[offset + statsSize - 1]
        );
    }
}
