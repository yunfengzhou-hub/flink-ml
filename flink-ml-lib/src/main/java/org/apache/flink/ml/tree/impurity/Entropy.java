package org.apache.flink.ml.tree.impurity;

public class Entropy implements Impurity{
    @Override
    public double calculate(double[] counts, double totalCount) {
        if (totalCount == 0) {
            return 0;
        }
        double impurity = 0.0;
        for (double classCount: counts) {
            if (classCount != 0) {
                double freq = classCount / totalCount;
                impurity -= freq * log2(freq);
            }
        }
        return impurity;
    }

    @Override
    public double calculate(double count, double sum, double sumSquares) {
        throw new UnsupportedOperationException("Entropy.calculate");
    }

    private double log2(double x) {
        return Math.log(x) / Math.log(2);
    }
}
