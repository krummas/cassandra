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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairedDataTombstonesTest extends CQLTester
{
    @Before
    public void setup() throws Throwable
    {
        DatabaseDescriptor.setOnlyPurgeRepairedTombstones(true);
    }
    @Test
    public void compactionTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2)) with gc_grace_seconds=0");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables().iterator().next();
        repair(repairedSSTable);

        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        verify();
        assertEquals(1, getCurrentColumnFamilyStore().getSSTables().size());
        assertFalse(getCurrentColumnFamilyStore().getSSTables().iterator().next().isRepaired());

    }

    @Test
    public void readTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0");
        for (int i = 0; i < 10; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables().iterator().next();
        repair(repairedSSTable);
        for (int i = 10; i < 20; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();

        // allow gcgrace to properly expire:
        Thread.sleep(1000);
        verify();

//        execute("select * from %s where id=?", 1);
    }

    @Test
    public void readTestRowTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables().iterator().next();
        repair(repairedSSTable);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        verify();
    }

    @Test
    public void readTestPartitionTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables().iterator().next();
        repairedSSTable.descriptor.getMetadataSerializer().mutateRepairedAt(repairedSSTable.descriptor, 1);
        repairedSSTable.reloadSSTableMetadata();
        getCurrentColumnFamilyStore().getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(repairedSSTable));
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();

        Thread.sleep(1000);
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int partitionsFound = 0;
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); UnfilteredPartitionIterator iterator = cmd.executeLocally(orderGroup))
        {
            while (iterator.hasNext())
            {
                partitionsFound++;
                UnfilteredRowIterator rowIter = iterator.next();
                int val = ByteBufferUtil.toInt(rowIter.partitionKey().getKey());
                assertTrue(val >= 10 && val < 20);
            }
        }
        assertEquals(10, partitionsFound);
    }

    private void verify()
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int foundRows = 0;
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); UnfilteredPartitionIterator iterator = cmd.executeLocally(orderGroup))
        {
            while (iterator.hasNext())
            {
                UnfilteredRowIterator rowIter = iterator.next();
                if (!rowIter.partitionKey().equals(Util.dk(ByteBufferUtil.bytes(999)))) // partition key 999 is 'live' and used to avoid sstables from being dropped
                {
                    while (rowIter.hasNext())
                    {
                        AbstractRow row = (AbstractRow) rowIter.next();
                        for (int i = 0; i < row.clustering().size(); i++)
                        {
                            foundRows++;
                            int val = ByteBufferUtil.toInt(row.clustering().get(i));
                            assertTrue("val=" + val, val >= 10 && val < 20);
                        }
                    }
                }
            }
        }
        assertEquals(foundRows, 10);
    }

    private void repair(SSTableReader sstable) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, 1);
        sstable.reloadSSTableMetadata();
        getCurrentColumnFamilyStore().getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
    }
}
