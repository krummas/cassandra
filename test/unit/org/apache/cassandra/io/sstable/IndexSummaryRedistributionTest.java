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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.metrics.StorageMetrics;

import static org.junit.Assert.assertEquals;

public class IndexSummaryRedistributionTest
{
    private static final String KEYSPACE1 = "IndexSummaryRedistributionTest";
    private static final String CF_STANDARD = "Standard";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingOptions.NONE));
    }

    @Test
    public void testMetricsLoadAfterRedistribution() throws IOException
    {
        String ksname = KEYSPACE1;
        String cfname = CF_STANDARD;
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        int numSSTables = 1;
        int numRows = 1024;
        long load = StorageMetrics.load.getCount();
        StorageMetrics.load.dec(load); // reset the load metric
        createSSTables(ksname, cfname, numSSTables, numRows);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());
        for (SSTableReader sstable : sstables)
            sstable.overrideReadMeter(new RestorableMeter(100.0, 100.0));

        long oldSize = 0;
        for (SSTableReader sstable : sstables)
        {
            assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);
            oldSize += sstable.bytesOnDisk();
        }

        load = StorageMetrics.load.getCount();
        assertEquals(oldSize, load, oldSize / 10);
        long others = load - oldSize; // Other SSTables size, e.g. schema and other system SSTables

        int originalMinIndexInterval = cfs.metadata.getMinIndexInterval();
        // double the min_index_interval
        cfs.metadata.minIndexInterval(originalMinIndexInterval * 2);
        IndexSummaryManager.instance.redistributeSummaries();

        long newSize = 0;
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertEquals(cfs.metadata.getMinIndexInterval(), sstable.getEffectiveIndexInterval(), 0.001);
            assertEquals(numRows / cfs.metadata.getMinIndexInterval(), sstable.getIndexSummarySize());
            newSize += sstable.bytesOnDisk();
        }
        newSize += others;
        load = StorageMetrics.load.getCount();

        // new size we calculate should be almost the same as the load in metrics
        assertEquals(newSize, load, newSize / 10);
    }

    private void createSSTables(String ksname, String cfname, int numSSTables, int numRows)
    {
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ArrayList<Future> futures = new ArrayList<>(numSSTables);
        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        for (int sstable = 0; sstable < numSSTables; sstable++)
        {
            for (int row = 0; row < numRows; row++)
            {
                DecoratedKey key = Util.dk(String.format("%3d", row));
                Mutation rm = new Mutation(ksname, key.getKey());
                rm.add(cfname, Util.cellname("column"), value, 0);
                rm.applyUnsafe();
            }
            futures.add(cfs.forceFlush());
        }
        for (Future future : futures)
        {
            try
            {
                future.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        assertEquals(numSSTables, cfs.getSSTables().size());
    }
}
