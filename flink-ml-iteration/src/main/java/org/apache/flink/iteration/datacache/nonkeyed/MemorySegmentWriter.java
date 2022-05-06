package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MemorySegmentWriter implements SegmentWriter {
    private final Path path;

    private final MemoryManager memoryManager;

    private MemorySegment currentMemorySegment;

    private final List<MemorySegment> bufferedMemorySegments;

    private int currentMemorySegmentOffset;

    private int currentSegmentCount;

    public MemorySegmentWriter(Path path, MemoryManager memoryManager) {
        this.path = path;
        this.memoryManager = memoryManager;
        this.bufferedMemorySegments = new ArrayList<>();
        createBufferedMemorySegment();
    }

    private void createBufferedMemorySegment() {
        try {
            currentMemorySegment = memoryManager.allocatePages(this, 1).get(0);
            currentMemorySegment.putInt(0, 0);
            bufferedMemorySegments.add(currentMemorySegment);
            currentMemorySegmentOffset = 4;
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean addRecord(byte[] bytes) throws IOException {
        if (currentMemorySegment.size() < currentMemorySegmentOffset + 20 + bytes.length) {
            createBufferedMemorySegment();
        }

        currentMemorySegment.putInt(currentMemorySegmentOffset, bytes.length);
        currentMemorySegmentOffset += 4;
        currentMemorySegment.put(currentMemorySegmentOffset, bytes);
        currentMemorySegmentOffset += bytes.length;
        currentMemorySegment.putInt(0, currentMemorySegment.getInt(0) + 1);

        currentSegmentCount++;
        return true;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        if (currentSegmentCount > 0) {
            Segment segment = new Segment(path, currentSegmentCount);
            segment.setBufferedMemorySegments(bufferedMemorySegments);
            return Optional.of(segment);
        } else {
            bufferedMemorySegments.forEach(memoryManager::release);
            return Optional.empty();
        }
    }
}
