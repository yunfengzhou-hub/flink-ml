package org.apache.flink.ml.regression.decisiontree;

import org.apache.flink.ml.param.HasPredictionCol;
import org.apache.flink.ml.param.shared.tree.HasImpurity;
import org.apache.flink.ml.param.shared.tree.HasMaxBins;
import org.apache.flink.ml.param.shared.tree.HasMaxDepth;
import org.apache.flink.ml.param.shared.colname.HasVarianceCol;

public interface DecisionTreeParams<T> extends
        HasVarianceCol<T>,
        HasImpurity<T>,
        HasMaxBins<T>,
        HasPredictionCol<T>,
        HasMaxDepth<T> {
}
