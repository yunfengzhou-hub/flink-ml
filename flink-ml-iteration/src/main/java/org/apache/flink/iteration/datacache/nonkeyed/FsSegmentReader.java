package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.core.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class FsSegmentReader implements SegmentReader {
    private final Segment segment;

    private final InputStream inputStream;

    private final ObjectInputStream objectInputStream;

    private int offset;

    public FsSegmentReader(Segment segment, int startOffset) throws IOException {
        this.segment = segment;
        FileSystem fileSystem = segment.getPath().getFileSystem();
        this.inputStream = fileSystem.open(segment.getPath());
        this.objectInputStream = new ObjectInputStream(inputStream);
        this.offset = startOffset;

        for (int i = 0; i < startOffset; i++) {
            next();
        }
    }

    @Override
    public boolean hasNext() {
        return offset < segment.getCount();
    }

    @Override
    public byte[] next() throws IOException {
        byte[] bytes;
        try {
            bytes = (byte[]) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        offset++;
        return bytes;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public int getOffset() {
        return offset;
    }
}
