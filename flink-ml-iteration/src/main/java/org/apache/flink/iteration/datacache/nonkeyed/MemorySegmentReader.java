package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.Iterator;

public class MemorySegmentReader implements SegmentReader {
    private final Segment segment;

    private final Iterator<MemorySegment> memorySegmentIterator;

    private int elementCount;

    private MemorySegment currentMemorySegment;

    private int currentSegmentElementCount;

    private int currentSegmentByteOffset;

    public MemorySegmentReader(Segment segment, int startOffset) {
        this.segment = segment;
        this.memorySegmentIterator = segment.getBufferedMemorySegment().iterator();
        this.elementCount = startOffset;
        this.currentSegmentByteOffset = 4;

        while (memorySegmentIterator.hasNext()) {
            currentMemorySegment = memorySegmentIterator.next();
            if (startOffset >= currentMemorySegment.getInt(0)) {
                startOffset -= currentMemorySegment.getInt(0);
            } else {
                break;
            }
        }

        for (currentSegmentElementCount = 0;
                currentSegmentElementCount < startOffset;
                currentSegmentElementCount++) {
            int length = currentMemorySegment.getInt(currentSegmentByteOffset);
            currentSegmentByteOffset += 4 + length;
        }
    }

    @Override
    public boolean hasNext() {
        return elementCount < segment.getCount();
    }

    @Override
    public byte[] next() throws IOException {
        int length = currentMemorySegment.getInt(currentSegmentByteOffset);
        //            System.out.printf("%d %d %d %d\n", currentMemorySegment.getInt(0),
        // currentSegmentByteOffset, length, currentSegmentElementCount);
        currentSegmentByteOffset += 4;

        byte[] bytes = new byte[length];
        currentMemorySegment.get(currentSegmentByteOffset, bytes);
        currentSegmentByteOffset += bytes.length;

        currentSegmentElementCount++;
        elementCount++;

        if (currentSegmentElementCount >= currentMemorySegment.getInt(0)) {
            if (memorySegmentIterator.hasNext()) {
                currentMemorySegment = memorySegmentIterator.next();
            } else {
                currentMemorySegment = null;
            }
            currentSegmentElementCount = 0;
            currentSegmentByteOffset = 4;
        }

        return bytes;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public int getOffset() {
        return elementCount;
    }
}
