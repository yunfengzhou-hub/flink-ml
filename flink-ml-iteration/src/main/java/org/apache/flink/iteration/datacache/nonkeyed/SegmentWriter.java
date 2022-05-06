package org.apache.flink.iteration.datacache.nonkeyed;

import java.io.IOException;
import java.util.Optional;

public interface SegmentWriter {
    boolean addRecord(byte[] bytes) throws IOException;

    Optional<Segment> finish() throws IOException;
}
