/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the behavior of {@link DataCache}. */
@RunWith(Parameterized.class)
public class DataCacheWriteReadTest extends TestLogger {

    @ClassRule public static final TemporaryFolder CLASS_TEMPORARY_FOLDER = new TemporaryFolder();

    private static MiniDFSCluster hdfsCluster;

    private final FileSystem fileSystem;

    private final Path basePath;

    @BeforeClass
    public static void createHDFS() throws Exception {
        Assume.assumeTrue(!OperatingSystem.isWindows());

        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set(
                MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
                CLASS_TEMPORARY_FOLDER.newFolder().getAbsolutePath());
        hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig).build();
    }

    @AfterClass
    public static void destroyHDFS() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
        }

        hdfsCluster = null;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] testData() {
        return new Object[][] {new Object[] {"local"}, new Object[] {"hdfs"}};
    }

    public DataCacheWriteReadTest(String fileSystemType) throws IOException {
        if (fileSystemType.equals("local")) {
            fileSystem = FileSystem.getLocalFileSystem();
            basePath = new Path("file://" + CLASS_TEMPORARY_FOLDER.newFolder().getAbsolutePath());
        } else if (fileSystemType.equals("hdfs")) {
            fileSystem = new HadoopFileSystem(hdfsCluster.getNewFileSystemInstance(0));
            basePath = new Path(hdfsCluster.getURI().toString() + "/" + UUID.randomUUID());
        } else {
            throw new UnsupportedEncodingException("Unsupported fs type: " + fileSystemType);
        }
    }

    @Test
    public void testWriteAndReadEmptyCache() throws IOException {
        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test." + UUID.randomUUID()));

        dataCache.finish();
        assertFalse(dataCache.iterator().hasNext());
    }

    @Test
    public void testWriteAndRead() throws IOException {
        final int numRecords = 10240;

        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test_single." + UUID.randomUUID()));
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(i);
        }

        dataCache.finish();
        List<Integer> read = new ArrayList<>();
        dataCache.iterator().forEachRemaining(read::add);

        assertEquals(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()), read);
    }

    @Test
    public void testGetIteratorPosition() throws Exception {
        final int numRecords = 10240;

        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test_single." + UUID.randomUUID()));
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(i);
        }

        dataCache.finish();
        DataCacheIterator<Integer> iterator = dataCache.iterator();

        for (int i = 0; i < 50; i++) {
            iterator.next();
        }

        assertEquals(50, iterator.getPos());
    }

    @Test
    public void testSetIteratorPosition() throws Exception {
        final int numRecords = 10240;

        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test_single." + UUID.randomUUID()));
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(i);
        }

        dataCache.finish();
        DataCacheIterator<Integer> iterator = dataCache.iterator();
        iterator.setPos(50);

        List<Integer> read = new ArrayList<>();
        iterator.forEachRemaining(read::add);

        assertEquals(IntStream.range(50, numRecords).boxed().collect(Collectors.toList()), read);
    }

    @Test
    public void testCacheInMemory() throws IOException {
        int numRecords = 10240;
        int pageSize = 4096;
        int pageNum = 64;

        MemorySegmentPool segmentPool =
                new LazyMemorySegmentPool(
                        this, MemoryManager.create(pageSize * pageNum, pageSize), pageNum);

        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test_single." + UUID.randomUUID()),
                        segmentPool);
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(i);
        }

        dataCache.finish();
        assertTrue(segmentPool.freePages() < pageNum);
        int freePages = segmentPool.freePages();

        for (int i = 0; i < 2; i++) {
            List<Integer> read = new ArrayList<>();
            dataCache.iterator().forEachRemaining(read::add);

            assertEquals(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()), read);
            assertEquals(freePages, segmentPool.freePages());
        }
    }

    @Test
    public void testInsufficientMemory() throws IOException {
        int numRecords = 10240;
        int pageSize = 4096;
        int pageNum = 4;
        MemorySegmentPool segmentPool =
                new LazyMemorySegmentPool(
                        this, MemoryManager.create(pageSize * pageNum, pageSize), pageNum);

        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "test_single." + UUID.randomUUID()),
                        segmentPool);
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(i);
        }

        dataCache.finish();
        assertTrue(segmentPool.freePages() < pageNum);
        int freePages = segmentPool.freePages();

        for (int i = 0; i < 2; i++) {
            List<Integer> read = new ArrayList<>();
            dataCache.iterator().forEachRemaining(read::add);

            assertEquals(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()), read);
            assertEquals(freePages, segmentPool.freePages());
        }
    }
}
