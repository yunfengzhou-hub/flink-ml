package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.param.BooleanParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

@SuppressWarnings("unchecked")
public interface OnlineKMeansParams<T> extends HasBatchStrategy<T>, KMeansParams<T> {
    Param<Boolean> INIT_RANDOM_CENTROIDS =
            new BooleanParam(
                    "initRandomCentroids",
                    "Whether to create random points as initial centroids.",
                    false
            );

    Param<Integer> DIMS =
            new IntParam(
                    "dims",
                    "The number of dimensions of centroids.",
                    1,
                    ParamValidators.gt(0));

    default boolean getInitRandomCentroids() {
        return get(INIT_RANDOM_CENTROIDS);
    }

    default T setInitRandomCentroids(boolean value) {
        set(INIT_RANDOM_CENTROIDS, value);
        return (T) this;
    }

    default int getDims() {
        return get(DIMS);
    }

    default T setDims(int value) {
        set(DIMS, value);
        return (T) this;
    }
}
