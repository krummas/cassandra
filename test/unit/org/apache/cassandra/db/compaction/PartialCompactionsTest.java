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
package org.apache.cassandra.db.compaction;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class PartialCompactionsTest extends SchemaLoader
{
    private static final String KEYSPACE = PartialCompactionsTest.class.getSimpleName();

    @Test
    public void shouldNotRemoveTombstonesShadowingDataExcludedFromCompaction() throws Exception
    {
        // given
        ColumnFamilyStore cfs = cfStore();
        int few = 10, many = 10 * few;

        // a large sstable as the oldest
        insert(cfs, 0, many);
        // more inserts (to have more than one sstable to compact)
        insert(cfs, many, many + few);
        // delete data that's in both of the sstables
        delete(cfs, many - few / 2, many + few / 2);

        // emulate there only being enough space to compact the smallest sstables
        long freeSpace = 1, maxSize = 0;
        for (SSTableReader ssTable : cfs.getLiveSSTables())
        {
            long size = ssTable.onDiskLength();
            if (size > maxSize) maxSize = size;
            freeSpace += size;
        }
        freeSpace -= maxSize;
        for (Directories.DataDirectory location : cfs.getDirectories().getWriteableLocations())
        {
            assertThat(location, instanceOf(WrappedDataDirectory.class));
            ((WrappedDataDirectory) location).availableSpace = freeSpace;
        }
        assertEquals("live sstables before compaction", 3, cfs.getLiveSSTables().size());

        // when - run a compaction where all tombstones have timed out
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));

        // then - the tombstones should not be removed
        assertEquals("live sstables after compaction", 2, cfs.getLiveSSTables().size());
        int rowCount = Util.getAll(Util.cmd(cfs, "key1").build()).stream()
                           .map(partition -> count(partition.rowIterator()))
                           .reduce(Integer::sum)
                           .orElse(0);

        assertEquals("remaining live rows after compaction", many, rowCount);
    }

    static int count(Iterator<?> iter)
    {
        try (CloseableIterator<?> unused = iter instanceof CloseableIterator ? (CloseableIterator<?>) iter : null)
        {
            int count = 0;
            for (; iter.hasNext();  iter.next())
            {
                count++;
            }
            return count;
        }
    }

    private static void insert(ColumnFamilyStore cfs, int firstKey, int endKey)
    {
        for (int i = firstKey; i < endKey; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), 0, "key1")
            .clustering(String.valueOf(i))
            .add("val", String.valueOf(i))
            .build()
            .applyUnsafe();
        }
        cfs.forceBlockingFlush();
    }

    private static void delete(ColumnFamilyStore cfs, int firstKey, int endKey)
    {
        for (int i = firstKey; i < endKey; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata(), 1, "key1", String.valueOf(i)).applyUnsafe();
        }
        cfs.forceBlockingFlush();
    }

    private ColumnFamilyStore cfStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        return keyspace.getColumnFamilyStore(CF());
    }

    private static class WrappedDataDirectory extends Directories.DataDirectory
    {
        Long availableSpace;

        WrappedDataDirectory(Directories.DataDirectory dataDirectory)
        {
            super(dataDirectory.location);
        }

        @Override
        public long getAvailableSpace()
        {
            if (availableSpace != null)
                return availableSpace;
            return super.getAvailableSpace();
        }
    }

    @Before
    public void prepareCF()
    {
        // Create the keyspace and table
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF()));
        // Create a new store for the table, with directories where we can manipulate the free space
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF());
        TableMetadataRef metadata = store.metadata;
        keyspace.dropCf(metadata.id);
        ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(keyspace, CF(), metadata, new Directories(metadata.get(), wrapDirectories(store.getDirectories().getWriteableLocations())), false, false, true);
        keyspace.initCfCustom(cfs);
    }

    @Rule
    public final TestName testName = new TestName();

    private String CF()
    {
        return testName.getMethodName();
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF());
        store.truncateBlocking();
        LifecycleTransaction.waitForDeletions();
    }

    @BeforeClass
    public static void initCassandraStaticState()
    {
        CompactionManager.instance.disableAutoCompaction();
    }

    private static Directories.DataDirectory[] wrapDirectories(Directories.DataDirectory[] dataDirectories)
    {
        Directories.DataDirectory[] result = new Directories.DataDirectory[dataDirectories.length];
        for (int i = 0; i < result.length; i++)
        {
            result[i] = new WrappedDataDirectory(dataDirectories[i]);
        }
        return result;
    }
}
