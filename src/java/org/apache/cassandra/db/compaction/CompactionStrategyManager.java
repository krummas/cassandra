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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;


public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private volatile AbstractCompactionStrategy repaired;
    private volatile AbstractCompactionStrategy unrepaired;
    private volatile boolean enabled = true;
    private boolean isActive = true;


    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        reload(cfs.metadata);
        cfs.getDataTracker().subscribe(this);
        logger.debug("{} subscribed to the data tracker.", this);
    }

    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        if (repaired.getEstimatedRemainingTasks() > unrepaired.getEstimatedRemainingTasks())
        {
            AbstractCompactionTask repairedTask = repaired.getNextBackgroundTask(gcBefore);
            if (repairedTask != null)
                return repairedTask;
            return unrepaired.getNextBackgroundTask(gcBefore);
        }
        else
        {
            AbstractCompactionTask unrepairedTask = unrepaired.getNextBackgroundTask(gcBefore);
            if (unrepairedTask != null)
                return unrepairedTask;
            return repaired.getNextBackgroundTask(gcBefore);
        }
    }


    public void disable()
    {
        enabled = false;
    }

    public void enable()
    {
        enabled = true;
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public void resume()
    {
        isActive = true;
    }

    public void pause()
    {
        isActive = false;
    }


    private void startup()
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (sstable.isRepaired())
                repaired.addSSTable(sstable);
            else
                unrepaired.addSSTable(sstable);
        }
        repaired.startup();
        unrepaired.startup();
    }

    public void shutdown()
    {
        isActive = false;
        repaired.shutdown();
        unrepaired.shutdown();
    }


    public synchronized void maybeReload(CFMetaData metadata)
    {
        if (repaired != null && repaired.getClass().equals(metadata.compactionStrategyClass)
                && unrepaired != null && unrepaired.getClass().equals(metadata.compactionStrategyClass)
                && repaired.options.equals(metadata.compactionStrategyOptions)
                && unrepaired.options.equals(metadata.compactionStrategyOptions))
            return;

        reload(metadata);
    }

    public synchronized void reload(CFMetaData metadata)
    {
        if (repaired != null)
            repaired.shutdown();
        if (unrepaired != null)
            unrepaired.shutdown();
        repaired = metadata.createCompactionStrategyInstance(cfs);
        unrepaired = metadata.createCompactionStrategyInstance(cfs);
        startup();
    }

    public void replaceFlushed(Memtable memtable, List<SSTableReader> sstables)
    {
        // flushed are always unrepaired
        unrepaired.replaceFlushed(memtable, sstables);
    }

    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        // todo: union of filtered sstables or intersection?
        return unrepaired.filterSSTablesForReads(repaired.filterSSTablesForReads(sstables));
    }

    public int getUnleveledSSTables()
    {
        if (this.repaired instanceof LeveledCompactionStrategy && this.unrepaired instanceof LeveledCompactionStrategy)
        {
            return ((LeveledCompactionStrategy) repaired).getLevelSize(0) + ((LeveledCompactionStrategy) unrepaired).getLevelSize(0);
        }
        return 0;
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        if (this.repaired instanceof LeveledCompactionStrategy && this.unrepaired instanceof LeveledCompactionStrategy)
        {
            int[] repairedCountPerLevel = ((LeveledCompactionStrategy) repaired).getAllLevelSize();
            int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) unrepaired).getAllLevelSize();
            return sumArrays(repairedCountPerLevel, unrepairedCountPerLevel);
        }
        return null;
    }

    public static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            if (flushedNotification.added.isRepaired())
                repaired.addSSTable(flushedNotification.added);
            else
                unrepaired.addSSTable(flushedNotification.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            for (SSTableReader sstable : listChangedNotification.removed)
            {
                if (sstable.isRepaired())
                    repaired.removeSSTable(sstable);
                else
                    unrepaired.removeSSTable(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                if (sstable.isRepaired())
                    repaired.addSSTable(sstable);
                else
                    unrepaired.addSSTable(sstable);
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstable)
            {
                if (sstable.isRepaired())
                {
                    unrepaired.removeSSTable(sstable);
                    repaired.addSSTable(sstable);
                }
                else
                {
                    repaired.removeSSTable(sstable);
                    unrepaired.addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            SSTableReader sstable = ((SSTableDeletingNotification) notification).deleting;
            if (sstable.isRepaired())
                repaired.removeSSTable(sstable);
            else
                unrepaired.removeSSTable(sstable);
        }
    }

    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> unrepairedSSTables = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            if (sstable.isRepaired())
                repairedSSTables.add(sstable);
            else
                unrepairedSSTables.add(sstable);
        ScannerList repairedScanners = repaired.getScanners(repairedSSTables, range);
        ScannerList unrepairedScanners = unrepaired.getScanners(unrepairedSSTables, range);
        List<ICompactionScanner> scanners = new ArrayList<>(repairedScanners.scanners.size() + unrepairedScanners.scanners.size());
        scanners.addAll(repairedScanners.scanners);
        scanners.addAll(unrepairedScanners.scanners);
        return new ScannerList(scanners);
    }

    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public long getMaxSSTableBytes()
    {
        return unrepaired.getMaxSSTableBytes();
    }

    public String getName()
    {
        return unrepaired.getName();
    }


    public AbstractCompactionTask getCompactionTask(Set<SSTableReader> input, int gcBefore, long maxSSTableBytes)
    {
        if (Iterables.all(input, new RepairedPredicate()))
            return repaired.getCompactionTask(input, gcBefore, maxSSTableBytes);
        else if (Iterables.all(input, Predicates.not(new RepairedPredicate())))
            return unrepaired.getCompactionTask(input, gcBefore, maxSSTableBytes);
        throw new UnsupportedOperationException("It is not supported doing compactions on repaired and unrepaired data.");
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore)
    {
                // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
            {
                synchronized (CompactionStrategyManager.this)
                {
                    AbstractCompactionTask repairedTask = repaired.getMaximalTask(gcBefore);
                    AbstractCompactionTask unrepairedTask = unrepaired.getMaximalTask(gcBefore);

                    if (repairedTask == null && unrepairedTask == null)
                        return null;
                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    if (repairedTask != null)
                        tasks.add(repairedTask);
                    if (unrepairedTask != null)
                        tasks.add(unrepairedTask);
                    return tasks;
                }
            }
        }, false);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        if (Iterables.all(sstables, new RepairedPredicate()))
            return repaired.getUserDefinedTask(sstables, gcBefore);
        else if (Iterables.all(sstables, Predicates.not(new RepairedPredicate())))
            return unrepaired.getUserDefinedTask(sstables, gcBefore);
        throw new UnsupportedOperationException("It is not supported doing compactions on repaired and unrepaired data.");
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> repairedSSTables)
    {
        return unrepaired.groupSSTablesForAntiCompaction(repairedSSTables);
    }

    public Integer getEstimatedRemainingTasks()
    {
        return repaired.getEstimatedRemainingTasks() + unrepaired.getEstimatedRemainingTasks();
    }

    public boolean shouldBeEnabled()
    {
        return unrepaired.shouldBeEnabled();
    }

    public boolean shouldDefragment()
    {
        return repaired.shouldDefragment();
    }

    private static class RepairedPredicate implements Predicate<SSTableReader>
    {
        @Override
        public boolean apply(SSTableReader sstable)
        {
            return sstable.isRepaired();
        }
    }
}
