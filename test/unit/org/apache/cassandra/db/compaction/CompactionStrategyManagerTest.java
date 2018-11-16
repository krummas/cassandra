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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

public class CompactionStrategyManagerTest
{
    @Test
    public void testGroupByStrategy() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        CompactionStrategyManager csm = new CompactionStrategyManager(cfs);
        List<SSTableReader> sstables = new ArrayList<>();

        int gen = 0;
        for (int i = 0; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(++gen, cfs);
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
            sstable.reloadSSTableMetadata();

            sstables.add(sstable);
        }
        Map<AbstractCompactionStrategy, Collection<SSTableReader>> group = csm.groupSSTablesByStrategy(sstables);
        assertEquals(1, group.size()); // only unrepaired ones
        for (int i = 0; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(++gen, cfs);
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, System.currentTimeMillis());
            sstable.reloadSSTableMetadata();

            sstables.add(sstable);
        }
        group = csm.groupSSTablesByStrategy(sstables);
        assertEquals(2, group.size());
        for (Map.Entry<AbstractCompactionStrategy, Collection<SSTableReader>> groupEntry : group.entrySet())
        {
            Collection<SSTableReader> groupSSTables = groupEntry.getValue();
            assertEquals(10, groupSSTables.size());
            boolean repaired = groupSSTables.iterator().next().isRepaired();
            assertTrue(groupSSTables.stream().allMatch(s -> s.isRepaired() == repaired));
        }
    }

    @Test
    public void testStartup() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        CompactionStrategyManager csm = new CompactionStrategyManager(cfs);
        List<SSTableReader> sstables = new ArrayList<>();

        int gen = 0;
        for (int i = 0; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(++gen, cfs);
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
            sstable.reloadSSTableMetadata();

            sstables.add(sstable);
        }
        for (int i = 0; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(++gen, cfs);
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, System.currentTimeMillis());
            sstable.reloadSSTableMetadata();

            sstables.add(sstable);
        }
        cfs.getTracker().addInitialSSTables(sstables);

        // make sure compaction strategies are empty before csm.startup():
        for (int i = 0; i < 2; i++)
        {
            AbstractCompactionStrategy strat = csm.getStrategies().get(i);
            Collection<AbstractCompactionTask> tasks = strat.getMaximalTask(0, false);
            assertNull(tasks);
        }
        // and make sure the new sstables are added to the correct strategy on startup:
        csm.startup();
        for (int i = 0; i < 2; i++)
        {
            AbstractCompactionStrategy strat = csm.getStrategies().get(i);
            boolean shouldBeRepaired = i == 0; // see CSM#getStrategies, repaired is in pos = 0

            // in trunk we have AbstractCompactionStrategy.getSSTables, avoid this there:
            Collection<AbstractCompactionTask> tasks = strat.getMaximalTask(0, false);
            assertEquals(1, tasks.size());
            AbstractCompactionTask task = tasks.iterator().next();
            assertEquals(10, task.transaction.originals().size());
            assertTrue(task.transaction.originals().stream().allMatch(s -> s.isRepaired() == shouldBeRepaired));
            task.transaction.abort();
        }
    }
}
