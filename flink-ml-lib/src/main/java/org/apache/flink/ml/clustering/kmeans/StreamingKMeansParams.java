package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.common.param.HasDecayFactor;
import org.apache.flink.ml.common.param.HasSeed;
import org.apache.flink.ml.param.*;

@SuppressWarnings("unchecked")
public interface StreamingKMeansParams<T> extends HasBatchStrategy<T>, HasDecayFactor<T>, HasSeed<T>, KMeansModelParams<T> {
    Param<String> INIT_MODE =
            new StringParam(
                    "initMode",
                    "How to initialize the model data of the online KMeans algorithm. Supported options: 'random', 'direct'.",
                    "random",
                    ParamValidators.inArray("random", "direct"));

    Param<Integer> DIMS =
            new IntParam(
                    "dims",
                    "The number of dimensions of centroids.",
                    1,
                    ParamValidators.gt(0));

    default String getInitMode() {
        return get(INIT_MODE);
    }

    default T setInitMode(String value) {
        set(INIT_MODE, value);
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
