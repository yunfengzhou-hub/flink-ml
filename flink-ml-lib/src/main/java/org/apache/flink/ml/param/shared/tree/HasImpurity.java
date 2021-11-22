package org.apache.flink.ml.param.shared.tree;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;
import org.apache.flink.ml.tree.impurity.Impurities;

public interface HasImpurity<T> extends WithParams<T> {
    Param<String> IMPURITY =
            new StringParam(
                    "impurityType",
                    "Type of impurity",
                    null,
                    ParamValidators.inArray(Impurities.getSupportedNames()));

    default String getImpurity() {
        return get(IMPURITY);
    }

    default T setImpurity(String value) {
        set(IMPURITY, value);
        return (T) this;
    }
}