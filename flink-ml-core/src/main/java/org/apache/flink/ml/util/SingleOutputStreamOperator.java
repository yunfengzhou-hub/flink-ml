package org.apache.flink.ml.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

@Public
public class SingleOutputStreamOperator<T> extends DataStream<T> {
    protected boolean nonParallel = false;
    private Map<OutputTag<?>, TypeInformation<?>> requestedSideOutputs = new HashMap();

    public SingleOutputStreamOperator(StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    public String getName() {
        return this.transformation.getName();
    }

    public SingleOutputStreamOperator<T> name(String name) {
        this.transformation.setName(name);
        return this;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> uid(String uid) {
        this.transformation.setUid(uid);
        return this;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> setUidHash(String uidHash) {
        this.transformation.setUidHash(uidHash);
        return this;
    }

    public SingleOutputStreamOperator<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, this.canBeParallel());
        this.transformation.setParallelism(parallelism);
        return this;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> setMaxParallelism(int maxParallelism) {
        OperatorValidationUtils.validateMaxParallelism(maxParallelism, this.canBeParallel());
        this.transformation.setMaxParallelism(maxParallelism);
        return this;
    }

    private SingleOutputStreamOperator<T> setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
        this.transformation.setResources(minResources, preferredResources);
        return this;
    }

    private SingleOutputStreamOperator<T> setResources(ResourceSpec resources) {
        this.transformation.setResources(resources, resources);
        return this;
    }

    private boolean canBeParallel() {
        return !this.nonParallel;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> forceNonParallel() {
        this.transformation.setParallelism(1);
        this.transformation.setMaxParallelism(1);
        this.nonParallel = true;
        return this;
    }

    public SingleOutputStreamOperator<T> setBufferTimeout(long timeoutMillis) {
        Preconditions.checkArgument(timeoutMillis >= -1L, "timeout must be >= -1");
        this.transformation.setBufferTimeout(timeoutMillis);
        return this;
    }

    @PublicEvolving
    private SingleOutputStreamOperator<T> setChainingStrategy(ChainingStrategy strategy) {
        if (this.transformation instanceof PhysicalTransformation) {
            ((PhysicalTransformation)this.transformation).setChainingStrategy(strategy);
            return this;
        } else {
            throw new UnsupportedOperationException("Cannot set chaining strategy on " + this.transformation);
        }
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> disableChaining() {
        return this.setChainingStrategy(ChainingStrategy.NEVER);
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> startNewChain() {
        return this.setChainingStrategy(ChainingStrategy.HEAD);
    }

    public SingleOutputStreamOperator<T> returns(Class<T> typeClass) {
        Objects.requireNonNull(typeClass, "type class must not be null.");

        try {
            return this.returns(TypeInformation.of(typeClass));
        } catch (InvalidTypesException var3) {
            throw new InvalidTypesException("Cannot infer the type information from the class alone.This is most likely because the class represents a generic type. In that case,please use the 'returns(TypeHint)' method instead.");
        }
    }

    public SingleOutputStreamOperator<T> returns(TypeHint<T> typeHint) {
        Objects.requireNonNull(typeHint, "TypeHint must not be null");

        try {
            return this.returns(TypeInformation.of(typeHint));
        } catch (InvalidTypesException var3) {
            throw new InvalidTypesException("Cannot infer the type information from the type hint. Make sure that the TypeHint does not use any generic type variables.");
        }
    }

    public SingleOutputStreamOperator<T> returns(TypeInformation<T> typeInfo) {
        Objects.requireNonNull(typeInfo, "TypeInformation must not be null");
        this.transformation.setOutputType(typeInfo);
        return this;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> slotSharingGroup(String slotSharingGroup) {
        this.transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<T> slotSharingGroup(SlotSharingGroup slotSharingGroup) {
        this.transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    public <X> DataStream<X> getSideOutput(OutputTag<X> sideOutputTag) {
        sideOutputTag = (OutputTag)this.clean(Objects.requireNonNull(sideOutputTag));
        sideOutputTag = new OutputTag(sideOutputTag.getId(), sideOutputTag.getTypeInfo());
        TypeInformation<?> type = (TypeInformation)this.requestedSideOutputs.get(sideOutputTag);
        if (type != null && !type.equals(sideOutputTag.getTypeInfo())) {
            throw new UnsupportedOperationException("A side output with a matching id was already requested with a different type. This is not allowed, side output ids need to be unique.");
        } else {
            this.requestedSideOutputs.put(sideOutputTag, sideOutputTag.getTypeInfo());
            SideOutputTransformation<X> sideOutputTransformation = new SideOutputTransformation(this.getTransformation(), sideOutputTag);
            return new DataStream(this.getExecutionEnvironment(), sideOutputTransformation);
        }
    }
}
