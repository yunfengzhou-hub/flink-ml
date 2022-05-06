package org.apache.flink.iteration.datacache.nonkeyed;

import java.io.IOException;

interface SegmentReader {
    boolean hasNext();

    byte[] next() throws IOException;

    void close() throws IOException;

    int getOffset();

    static SegmentReader create(Segment segment, int startOffset) throws IOException {
        if (segment.isInMemory()) {
            return new MemorySegmentReader(segment, startOffset);
        }
        return new FsSegmentReader(segment, startOffset);
    }
}
