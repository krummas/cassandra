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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.db.view.ViewBuilderTask;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ActiveCompactionsTest extends CQLTester
{
    @Test
    public void testSecondaryIndexTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        String idxName = createIndex("CREATE INDEX on %s(a)");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES ("+i+", 2, 3, 4)");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        Index idx = getCurrentColumnFamilyStore().indexManager.getIndexByName(idxName);
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        SecondaryIndexBuilder builder = idx.getBuildTaskSupport().getIndexBuildTask(getCurrentColumnFamilyStore(), Collections.singleton(idx), sstables);

        MockTracker mockTracker = new MockTracker();
        CompactionManager.instance.submitIndexBuild(builder, mockTracker).get();

        assertTrue(mockTracker.finished);
        assertNotNull(mockTracker.holder);
        assertEquals(sstables, mockTracker.holder.getCompactionInfo().getSSTables());
    }

    @Test
    public void testIndexSummaryRedistributionTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES ("+i+", 2, 3, 4)");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(sstables, OperationType.INDEX_SUMMARY))
        {
            Map<TableId, LifecycleTransaction> transactions = ImmutableMap.<TableId, LifecycleTransaction>builder().put(getCurrentColumnFamilyStore().metadata().id, txn).build();
            IndexSummaryRedistribution isr = new IndexSummaryRedistribution(new ArrayList<>(sstables), transactions, 1000);
            MockTracker mockTracker = new MockTracker();
            CompactionManager.instance.runIndexSummaryRedistribution(isr, mockTracker);
            assertTrue(mockTracker.finished);
            assertNotNull(mockTracker.holder);
            assertEquals(sstables, mockTracker.holder.getCompactionInfo().getSSTables());
        }
    }

    @Test
    public void testViewBuildTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (k1, c1, val) VALUES ("+i+", 2, 3)");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }
        execute(String.format("CREATE MATERIALIZED VIEW %s.view1 AS SELECT k1, c1, val FROM %s.%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)", keyspace(), keyspace(), currentTable()));
        View view = Iterables.getOnlyElement(getCurrentColumnFamilyStore().viewManager);

        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        ViewBuilderTask vbt = new ViewBuilderTask(getCurrentColumnFamilyStore(), view, new Range<>(token, token), token, 0);

        MockTracker tracker = new MockTracker();
        CompactionManager.instance.submitViewBuilder(vbt, tracker).get();
        assertTrue(tracker.finished);
        assertTrue(tracker.holder.getCompactionInfo().getSSTables().isEmpty());
        // this should stop for all compactions, even if it doesn't pick any sstables;
        assertTrue(tracker.holder.getCompactionInfo().shouldStop((sstable) -> false));
    }

    @Test
    public void testScrubOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        SSTableReader sstable = Iterables.getFirst(getCurrentColumnFamilyStore().getLiveSSTables(), null);
        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(sstable, OperationType.SCRUB))
        {
            MockTracker tracker = new MockTracker();
            CompactionManager.instance.scrubOne(getCurrentColumnFamilyStore(), txn, true, false, false, tracker);

            assertTrue(tracker.finished);
            assertEquals(tracker.holder.getCompactionInfo().getSSTables(), Sets.newHashSet(sstable));
            assertFalse(tracker.holder.getCompactionInfo().shouldStop((s) -> false));
            assertTrue(tracker.holder.getCompactionInfo().shouldStop((s) -> true));
        }

    }

    @Test
    public void testVerifyOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        SSTableReader sstable = Iterables.getFirst(getCurrentColumnFamilyStore().getLiveSSTables(), null);
        MockTracker tracker = new MockTracker();
        CompactionManager.instance.verifyOne(getCurrentColumnFamilyStore(), sstable, new Verifier.Options.Builder().build(), tracker);
        assertTrue(tracker.finished);
        assertEquals(tracker.holder.getCompactionInfo().getSSTables(), Sets.newHashSet(sstable));
        assertFalse(tracker.holder.getCompactionInfo().shouldStop((s) -> false));
        assertTrue(tracker.holder.getCompactionInfo().shouldStop((s) -> true));
    }

    @Test
    public void testSubmitCacheWrite() throws ExecutionException, InterruptedException
    {
        AutoSavingCache.Writer writer = CacheService.instance.keyCache.getWriter(100);
        MockTracker tracker = new MockTracker();
        CompactionManager.instance.submitCacheWrite(writer, tracker).get();
        assertTrue(tracker.finished);
        assertTrue(tracker.holder.getCompactionInfo().getSSTables().isEmpty());
    }

    private static class MockTracker implements ActiveCompactionsTracker
    {
        public CompactionInfo.Holder holder;
        public boolean finished = false;
        public void beginCompaction(CompactionInfo.Holder ci)
        {
            holder = ci;
        }

        public void finishCompaction(CompactionInfo.Holder ci)
        {
            finished = true;
        }
    }
}
