package org.apache.flink.ml.common.param;

import org.apache.flink.ml.param.*;

@SuppressWarnings("unchecked")
public interface HasDecayFactor<T> extends WithParams<T> {
    String BATCH_UNIT = "batches";
    String POINT_UNIT = "points";

    Param<String> HALF_LIFE_TIME_UNIT =
            new StringParam(
                    "halfLifeTimeUnit",
                    "The time unit of half life. " +
                            "If points, then the decay factor is raised to the power of number of new points. " +
                            "If batches, then decay factor will be used as is.",
                    BATCH_UNIT,
                    ParamValidators.inArray(BATCH_UNIT, POINT_UNIT));


    Param<Double> DECAY_FACTOR =
            new DoubleParam(
                    "decayFactor",
                    "The forgetfulness of the previous centroids.",
                    0.,
                    ParamValidators.gtEq(0));

    default double getDecayFactor() {
        return get(DECAY_FACTOR);
    }

    default T setDecayFactor(Double value) {
        return set(DECAY_FACTOR, value);
    }

    default String getTimeUnit() {
        return get(HALF_LIFE_TIME_UNIT);
    }

    default T setHalfLife(double halfLife, String timeUnit) {
        if (halfLife < 0.0) {
            throw new IllegalArgumentException(
                    "Parameter halfLife is given an invalid value " + halfLife);
        }
        set(DECAY_FACTOR, Math.exp(Math.log(0.5) / halfLife));
        set(HALF_LIFE_TIME_UNIT, timeUnit);
        return (T) this;
    }
}