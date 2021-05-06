package org.apache.flink.ml.common.function;

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

public class SourceEmbedVertex extends EmbedVertex{
    protected SourceEmbedVertex(StreamNode node, EmbedOutput<StreamRecord> output) {
        super(node, output);
    }

    @Override
    public List<StreamRecord> getInputList(int typeNumber){
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        output.getOutputList().clear();
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException();
    }
}
