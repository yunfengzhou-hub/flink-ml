package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;

public class FsSegmentWriter implements SegmentWriter {
    private final FileSystem fileSystem;

    private final Path path;

    private final FSDataOutputStream outputStream;

    private final ObjectOutputStream objectOutputStream;

    private final ByteArrayOutputStream byteArrayOutputStream;

    private int currentSegmentCount;

    private int currentSegmentSize;

    public FsSegmentWriter(Path path) throws IOException {
        this.path = path;
        this.fileSystem = path.getFileSystem();
        this.outputStream = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        this.objectOutputStream = new ObjectOutputStream(outputStream);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.currentSegmentSize = 0;
    }

    @Override
    public boolean addRecord(byte[] bytes) throws IOException {
        if (currentSegmentSize + bytes.length > 1024 * 1024 * 16) {
            return false;
        }

        objectOutputStream.writeObject(bytes);
        byteArrayOutputStream.reset();
        currentSegmentCount += 1;
        currentSegmentSize += bytes.length;
        return true;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        this.objectOutputStream.flush();
        this.outputStream.flush();
        this.outputStream.close();

        if (currentSegmentCount > 0) {
            return Optional.of(new Segment(path, currentSegmentCount));
        } else {
            // If there are no records, we tend to directly delete this file
            fileSystem.delete(path, false);
            return Optional.empty();
        }
    }
}
