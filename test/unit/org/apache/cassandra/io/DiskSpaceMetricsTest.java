package org.apache.cassandra.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.fail;

public class DiskSpaceMetricsTest extends CQLTester
{
    /**
     * This test runs the system with normal operations and makes sure the disk metrics match reality
     */
    @Test
    public void baseline() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, PRIMARY KEY (pk)) WITH min_index_interval=1");
        ColumnFamilyStore table = getCurrentColumnFamilyStore();

        // disable compaction so nothing changes between calculations
        table.disableAutoCompaction();

        // create 100 sstables
        for (int i = 0; i < 100; i++)
            insert(table, i);
        assertDiskSpaceEqual(table);
    }

    /**
     * If index summary downsampling is interrupted in the middle, the metrics still reflect the real data
     */
    @Test
    public void summaryRedistribution() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, PRIMARY KEY (pk)) WITH min_index_interval=1");
        ColumnFamilyStore table = getCurrentColumnFamilyStore();

        // disable compaction so nothing changes between calculations
        table.disableAutoCompaction();

        // create 100 sstables, make sure they have more than 1 value, else sampling can't happen
        for (int i = 0; i < 100; i++)
            insertN(table, 10, i);
        assertDiskSpaceEqual(table);

        // summary downsample
        for (int i = 0; i < 100; i++)
        {
            indexDownsampleCancelLastSSTable(table);
            assertDiskSpaceEqual(table);
        }
    }

    private void insert(ColumnFamilyStore table, long value) throws Throwable
    {
        insertN(table, 1, value);
    }

    private void insertN(ColumnFamilyStore table, int n, long base) throws Throwable
    {
        for (int i = 0; i < n; i++)
            execute("INSERT INTO %s (pk) VALUES (?)", base + i);

        // flush to write the sstable
        table.forceBlockingFlush();
    }

    private void assertDiskSpaceEqual(ColumnFamilyStore table)
    {
        long liveDiskSpaceUsed = table.metric.liveDiskSpaceUsed.getCount();
        long actual = 0;
        for (SSTableReader sstable : table.getTracker().getView().liveSSTables())
            actual += sstable.bytesOnDisk();

        Assert.assertEquals("bytes on disk does not match current metric liveDiskSpaceUsed", actual, liveDiskSpaceUsed);

        // totalDiskSpaceUsed is based off SStable delete, which is async: LogTransaction's tidy enqueues in ScheduledExecutors.nonPeriodicTasks
        // wait for there to be no more pending sstable releases
        LifecycleTransaction.waitForDeletions();
        while (table.metric.totalDiskSpaceUsed.getCount() != actual)
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    private static void indexDownsampleCancelLastSSTable(ColumnFamilyStore cfs)
    {
        List<SSTableReader> sstables = Lists.newArrayList(cfs.getSSTables(SSTableSet.CANONICAL));
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
        Map<TableId, LifecycleTransaction> txns = ImmutableMap.of(cfs.metadata.id, txn);
        // fail on the last file (* 3 because we call isStopRequested 3 times for each sstable, and we should fail on the last)
        AtomicInteger countdown = new AtomicInteger(3 * sstables.size() - 1);
        IndexSummaryRedistribution redistribution = new IndexSummaryRedistribution(txns, 0, 0) {
            public boolean isStopRequested()
            {
                return countdown.decrementAndGet() == 0;
            }
        };
        try
        {
            IndexSummaryManager.redistributeSummaries(redistribution);
            fail("Should throw CompactionInterruptedException");
        }
        catch (CompactionInterruptedException e)
        {
            // trying to get this to happen
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            try
            {
                FBUtilities.closeAll(txns.values());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
