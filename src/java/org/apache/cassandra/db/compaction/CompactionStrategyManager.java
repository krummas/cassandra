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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
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
    private volatile List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private volatile List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
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

        List<AbstractCompactionStrategy> strategies = new ArrayList<>(repaired.size() + unrepaired.size());
        strategies.addAll(repaired);
        strategies.addAll(unrepaired);
        Collections.sort(strategies, new Comparator<AbstractCompactionStrategy>()
        {
            @Override
            public int compare(AbstractCompactionStrategy o1, AbstractCompactionStrategy o2)
            {
                return Ints.compare(o2.getEstimatedRemainingTasks(), o1.getEstimatedRemainingTasks());
            }
        });
        for (AbstractCompactionStrategy strategy : strategies)
        {
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            if (task != null)
                return task;
        }
        return null;
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
            getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.startup();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.startup();
    }

    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        Directories.DataDirectory[] directories = cfs.directories.getWriteableLocations();
        int index = getDirectoryIndexFor(sstable.descriptor, directories);
        if (sstable.isRepaired())
            return repaired.get(index);
        else
            return unrepaired.get(index);
    }

    public static int getDirectoryIndexFor(Descriptor descriptor, Directories.DataDirectory[] directories)
    {
        for (int i = 0; i < directories.length; i++)
        {
            Directories.DataDirectory directory = directories[i];
            if (descriptor.directory.getPath().startsWith(directory.location.getPath()))
                return i;
        }
        logger.warn("Could not find directory for {} - putting it in {} instead",descriptor, directories[0].location);
        return 0;
    }

    public void shutdown()
    {
        isActive = false;
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.shutdown();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.shutdown();

    }


    public synchronized void maybeReload(CFMetaData metadata)
    {
        if (repaired != null && repaired.getClass().equals(metadata.compactionStrategyClass)
                && unrepaired != null && unrepaired.getClass().equals(metadata.compactionStrategyClass)
                && repaired.get(0).options.equals(metadata.compactionStrategyOptions) // todo: assumes all have the same options
                && unrepaired.get(0).options.equals(metadata.compactionStrategyOptions))
            return;

        reload(metadata);
    }

    public synchronized void reload(CFMetaData metadata)
    {
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.shutdown();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.shutdown();
        repaired.clear();
        unrepaired.clear();
        for (int i = 0; i < cfs.directories.getWriteableLocations().length; i++)
        {
            repaired.add(metadata.createCompactionStrategyInstance(cfs));
            unrepaired.add(metadata.createCompactionStrategyInstance(cfs));
        }
        startup();
    }

    public void replaceFlushed(Memtable memtable, List<SSTableReader> sstables)
    {
        cfs.getDataTracker().replaceFlushed(memtable, sstables);
        if (sstables != null)
            CompactionManager.instance.submitBackground(cfs);
    }

    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        // todo: union of filtered sstables or intersection?
        return unrepaired.get(0).filterSSTablesForReads(repaired.get(0).filterSSTablesForReads(sstables));
    }

    public int getUnleveledSSTables()
    {
        if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
        {
            int count = 0;
            for (AbstractCompactionStrategy strategy : repaired)
                count += ((LeveledCompactionStrategy)strategy).getLevelSize(0);
            for (AbstractCompactionStrategy strategy : unrepaired)
                count += ((LeveledCompactionStrategy)strategy).getLevelSize(0);
            return count;
        }
        return 0;
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
        {
            int [] res = new int[15];
            for (AbstractCompactionStrategy strategy : repaired)
            {
                int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                res = sumArrays(res, repairedCountPerLevel);
            }
            for (AbstractCompactionStrategy strategy : unrepaired)
            {
                int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                res = sumArrays(res, unrepairedCountPerLevel);
            }

            return res;
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
            getCompactionStrategyFor(flushedNotification.added).addSSTable(flushedNotification.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            for (SSTableReader sstable : listChangedNotification.removed)
            {
                getCompactionStrategyFor(sstable).removeSSTable(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                getCompactionStrategyFor(sstable).addSSTable(sstable);
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstable)
            {
                int index = getDirectoryIndexFor(sstable.descriptor, cfs.directories.getWriteableLocations());
                if (sstable.isRepaired())
                {
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).addSSTable(sstable);
                }
                else
                {
                    repaired.get(index).removeSSTable(sstable);
                    unrepaired.get(index).addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            SSTableReader sstable = ((SSTableDeletingNotification) notification).deleting;
            getCompactionStrategyFor(sstable).removeSSTable(sstable);
        }
    }

    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        List<ICompactionScanner> scanners = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            scanners.addAll(getCompactionStrategyFor(sstable).getScanners(Arrays.asList(sstable), range).scanners);
        return new ScannerList(scanners);
    }

    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public long getMaxSSTableBytes()
    {
        return unrepaired.get(0).getMaxSSTableBytes();
    }

    public String getName()
    {
        return unrepaired.get(0).getName();
    }


    public AbstractCompactionTask getCompactionTask(Set<SSTableReader> input, int gcBefore, long maxSSTableBytes)
    {
        validateForCompaction(input);
        return getCompactionStrategyFor(input.iterator().next()).getCompactionTask(input, gcBefore, maxSSTableBytes);
    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        assert !Iterables.isEmpty(input);
        SSTableReader firstSSTable = input.iterator().next();
        boolean repaired = firstSSTable.isRepaired();
        int firstIndex = getDirectoryIndexFor(firstSSTable.descriptor, cfs.directories.getWriteableLocations());
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (firstIndex != getDirectoryIndexFor(sstable.descriptor, cfs.directories.getWriteableLocations()))
                throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
        }
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
                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    for (AbstractCompactionStrategy strategy : repaired)
                    {
                        AbstractCompactionTask task = strategy.getMaximalTask(gcBefore);
                        if (task != null)
                            tasks.add(task);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired)
                    {
                        AbstractCompactionTask task = strategy.getMaximalTask(gcBefore);
                        if (task != null)
                            tasks.add(task);
                    }
                    return tasks;
                }
            }
        }, false);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        validateForCompaction(sstables);
        return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> repairedSSTables)
    {
        return null; //TODO!! unrepaired.groupSSTablesForAntiCompaction(repairedSSTables);
    }

    public int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (AbstractCompactionStrategy strategy : repaired)
            tasks += strategy.getEstimatedRemainingTasks();
        for (AbstractCompactionStrategy strategy : unrepaired)
            tasks += strategy.getEstimatedRemainingTasks();

        return tasks;
    }

    public boolean shouldBeEnabled()
    {
        return unrepaired.get(0).shouldBeEnabled();
    }

    public boolean shouldDefragment()
    {
        return repaired.get(0).shouldDefragment();
    }
}
