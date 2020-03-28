package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class DiskSpaceMetricsTest extends TestBaseImpl
{
    /**
     * This test runs the system with normal operations and makes sure the disk metrics match reality
     */
    @Test
    public void baseline() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            String tableName = "baseline";
            IInvokableInstance instance = cluster.get(1);
            instance.executeInternal("CREATE TABLE " + KEYSPACE + ".baseline (pk bigint, PRIMARY KEY (pk)) WITH min_index_interval=1");

            // disable compaction so nothing changes between calculations
            instance.nodetoolResult("disableautocompaction", KEYSPACE, tableName).asserts().success();

            // create 100 sstables
            for (int i = 0; i < 100; i++)
                insert(cluster, KEYSPACE, tableName, i);
            assertDiskSpaceEqual(instance, tableName);

            // run compaction to make sure metrics stay correct
            for (int i = 0; i < 10; i++)
            {
                instance.forceCompact(KEYSPACE, tableName);
                assertDiskSpaceEqual(instance, tableName);
            }

            // summary downsample
            for (int i = 0; i < 10; i++)
            {
                instance.runOnInstance(() -> {
                    try
                    {
                        IndexSummaryManager.instance.redistributeSummaries();
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                assertDiskSpaceEqual(instance, tableName);
            }
        }
    }

    /**
     * If index summary downsampling is interrupted in the middle, the metrics still reflect the real data
     */
    @Test
    public void summaryRedistribution() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            String tableName = "summary_redistribution";
            IInvokableInstance instance = cluster.get(1);
            instance.executeInternal("CREATE TABLE " + KEYSPACE + ".summary_redistribution (pk bigint, PRIMARY KEY (pk)) WITH min_index_interval=1");

            // disable compaction so nothing changes between calculations
            instance.nodetoolResult("disableautocompaction", KEYSPACE, tableName).asserts().success();

            // create 100 sstables, make sure they have more than 1 value, else sampling can't happen
            for (int i = 0; i < 100; i++)
                insertN(cluster, KEYSPACE, tableName, 10, i);
            assertDiskSpaceEqual(instance, tableName);

            // summary downsample
            for (int i = 0; i < 100; i++)
            {
                instance.runOnInstance(() -> indexDownsampleCancelLastSSTable(KEYSPACE, tableName));
                assertDiskSpaceEqual(instance, tableName);
            }
        }
    }

    private static void insert(Cluster cluster, String keyspace, String table, long value)
    {
        insertN(cluster, keyspace, table, 1, value);
    }

    private static void insertN(Cluster cluster, String keyspace, String table, int n, long base)
    {
        IInvokableInstance node = cluster.get(1);
        for (int i = 0; i < n; i++)
            node.executeInternal("INSERT INTO " + keyspace + "." + table + " (pk) VALUES (?)", base + i);

        // flush to write the sstable
        node.flush(keyspace);
    }

    private void assertDiskSpaceEqual(IInvokableInstance instance, String tableName)
    {
        //TODO jvm-dtest doesn't support jmx values, so need to exec
        long liveDiskSpaceUsed = instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).metric.liveDiskSpaceUsed.getCount());

        long actual = instance.callOnInstance(() -> {
            ColumnFamilyStore cf = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
            long count = 0;
            for (SSTableReader sstable : cf.getTracker().getView().liveSSTables())
                count += sstable.bytesOnDisk();

            return count;
        });

        Assert.assertEquals("bytes on disk does not match current metric liveDiskSpaceUsed", actual, liveDiskSpaceUsed);

        // totalDiskSpaceUsed is based off SStable delete, which is async: LogTransaction's tidy enqueues in ScheduledExecutors.nonPeriodicTasks
        // wait for there to be no more pending sstable releases
        instance.runOnInstance(() -> {
            ColumnFamilyStore cf = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
            while (cf.metric.pendingSSTableReleases.getCount() > 0) {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        });
        long totalDiskSpaceUsed = instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).metric.totalDiskSpaceUsed.getCount());
        Assert.assertEquals("bytes on disk does not match current metric totalDiskSpaceUsed", actual, totalDiskSpaceUsed);
    }

    private static void indexDownsampleCancelLastSSTable(String keyspace, String tableName)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(tableName);
        View view = store.getTracker().getView();
        List<SSTableReader> sstables = Lists.newArrayList(view.select(SSTableSet.CANONICAL));
        LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.UNKNOWN);
        Map<TableId, LifecycleTransaction> txns = ImmutableMap.of(store.metadata.id, txn);
        long nonRedistributingOffHeapSize = sstables.stream().mapToLong(SSTableReader::getIndexSummaryOffHeapSize).sum();
        // fail on the last file
        AtomicInteger countdown = new AtomicInteger(sstables.size() - 1);
        SimpleCondition condition = new SimpleCondition();
        IndexSummaryRedistribution redistribution = new IndexSummaryRedistribution(txns, nonRedistributingOffHeapSize, 0, new IndexSummaryRedistribution.Listener()
        {
            public void onPreResample(SSTableReader sstable)
            {
                if (countdown.decrementAndGet() == 0)
                {
                    // winner, block waiting for the pool
                    try
                    {
                        condition.await();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        try
        {
            ForkJoinPool.commonPool().execute(() -> {
                while (countdown.get() > 0) { }
                redistribution.stop();
                condition.signalAll();
            });
            IndexSummaryManager.redistributeSummaries(redistribution);
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
