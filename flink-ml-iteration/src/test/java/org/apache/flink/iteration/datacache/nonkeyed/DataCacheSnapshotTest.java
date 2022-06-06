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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/** Tests the behavior of the {@link DataCache}. */
@RunWith(Parameterized.class)
public class DataCacheSnapshotTest extends TestLogger {

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

    public DataCacheSnapshotTest(String fileSystemType) throws IOException {
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
    public void testSnapshot() throws Exception {
        int numRecords = 600;
        DataCache<Integer> dataCache = createDataCacheAndAddRecords(numRecords);
        checkWriteAndRecoverAndReplay(numRecords, dataCache);
    }

    @Test
    public void testSnapshotMultipleDataCachesIntoSingleStream() throws Exception {
        int numRecords = 600;
        DataCache<Integer> dataCache1 = createDataCacheAndAddRecords(numRecords);
        DataCache<Integer> dataCache2 = createDataCacheAndAddRecords(numRecords);

        checkWriteAndRecoverAndReplay(numRecords, dataCache1, dataCache2);
    }

    private DataCache<Integer> createDataCacheAndAddRecords(int numRecords) throws IOException {
        DataCache<Integer> dataCache =
                new DataCache<>(
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "dataCache." + UUID.randomUUID()));
        int nextNumber = 0;
        for (int i = 0; i < numRecords; ++i) {
            dataCache.addRecord(nextNumber++);
        }
        return dataCache;
    }

    @SafeVarargs
    private final void checkWriteAndRecoverAndReplay(
            int numRecords, DataCache<Integer>... dataCaches) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (DataCache<?> dataCache : dataCaches) {
            dataCache.writeTo(bos);
        }

        byte[] data = bos.toByteArray();

        ByteArrayInputStream recoverInputStream = new ByteArrayInputStream(data);
        for (DataCache<Integer> dataCache : dataCaches) {
            checkRecover(dataCache, recoverInputStream);
        }

        ByteArrayInputStream replayInputStream = new ByteArrayInputStream(data);
        for (DataCache<?> ignored : dataCaches) {
            checkReplay(replayInputStream, numRecords);
        }
    }

    private void checkRecover(DataCache<Integer> dataCache, InputStream inputStream)
            throws IOException {
        DataCache<Integer> copied =
                DataCache.recover(
                        inputStream,
                        IntSerializer.INSTANCE,
                        fileSystem,
                        () -> new Path(basePath, "dataCache." + UUID.randomUUID()));
        assertEquals(readElements(dataCache), readElements(copied));
    }

    private void checkReplay(InputStream inputStream, int numRecords) throws Exception {
        List<Integer> elements = new ArrayList<>();
        DataCache.replay(inputStream, IntSerializer.INSTANCE, elements::add);

        int totalRecords = IntStream.of(numRecords).sum();
        assertEquals(
                IntStream.range(0, totalRecords).boxed().collect(Collectors.toList()), elements);
    }

    private List<Integer> readElements(DataCache<Integer> dataCache) {
        List<Integer> list = new ArrayList<>();
        dataCache.iterator().forEachRemaining(list::add);
        return list;
    }
}
