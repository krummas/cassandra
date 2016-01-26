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

package org.apache.cassandra.cql3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

public class GcCompactionTest extends CQLTester
{
    static final int KEY_COUNT = 10;
    static final int CLUSTERING_COUNT = 20;

    Set<SSTableReader> readers;

    @Before
    public void before() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                "  key int," +
                "  column int," +
                "  data int," +
                "  extra text," +
                "  PRIMARY KEY(key, column)" +
                ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones' : 'true'  };"
                );

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i+j, "" + i + ":" + j);

        readers = new HashSet<>();
    }

    @Test
    public void testGcCompaction() throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable();
        assertEquals(0, countTombstoneMarkers(table0));
        int rowCount = countRows(table0);

        deleteWithSomeInserts(3, 5, 10);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable();
        assertTrue(countRows(table1) > 0);
        assertTrue(countTombstoneMarkers(table1) > 0);

        deleteWithSomeInserts(5, 6, 0);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());
        SSTableReader table2 = getNewTable();
        assertEquals(0, countRows(table2));
        assertTrue(countTombstoneMarkers(table2) > 0);

        CompactionManager.instance.forceUserDefinedCompaction(table0.getFilename());

        assertEquals(3, cfs.getLiveSSTables().size());
        SSTableReader table3 = getNewTable();
        assertEquals(0, countTombstoneMarkers(table3));
        assertTrue(rowCount > countRows(table3));
    }

    private SSTableReader getNewTable()
    {
        Set<SSTableReader> newOnes = new HashSet<>(getCurrentColumnFamilyStore().getLiveSSTables());
        newOnes.removeAll(readers);
        assertEquals(1, newOnes.size());
        readers.addAll(newOnes);
        return Iterables.get(newOnes, 0);
    }

    void deleteWithSomeInserts(int key_step, int clustering_step, int readd_step) throws Throwable
    {
        for (int i = 0; i < KEY_COUNT; i += key_step)
            for (int j = 0; j < CLUSTERING_COUNT; j += clustering_step)
            {
                execute("DELETE FROM %s WHERE key = ? AND column = ?", i, j);
                if (readd_step > 0 && j % readd_step == 0)
                    execute("INSERT INTO %s (key, column, extra) VALUES (?, ?, ?)", i, j, "readded " + i + ":" + j);
            }
    }

    int countTombstoneMarkers(SSTableReader reader)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        return count(reader, x -> x.isRangeTombstoneMarker() || x.isRow() && ((Row) x).hasDeletion(nowInSec));
    }

    int countRows(SSTableReader reader)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        return count(reader, x -> x.isRow() && ((Row) x).hasLiveData(nowInSec));
    }

    int count(SSTableReader reader, Predicate<Unfiltered> predicate)
    {
        int instances = 0;
        try (ISSTableScanner partitions = reader.getScanner())
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator iter = partitions.next())
                {
                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        if (predicate.test(atom))
                            ++instances;
                    }
                }
            }
        }
        return instances;
    }
}