package org.apache.flink.ml.tree;

import org.apache.flink.ml.linalg.Vector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CategoricalSplit implements Split{
    private final boolean isLeft;
    private final int featureIndex;
    private final Set<Double> categories;

    public CategoricalSplit(boolean isLeft, int featureIndex, Double... categories) {
        this.isLeft = isLeft;
        this.featureIndex = featureIndex;
        this.categories = new HashSet<>(Arrays.asList(categories));
    }

    @Override
    public boolean shouldGoLeft(Vector features) {
        if (isLeft) {
            return categories.contains(features.get(featureIndex));
        } else {
            return !categories.contains(features.get(featureIndex));
        }
    }
}
