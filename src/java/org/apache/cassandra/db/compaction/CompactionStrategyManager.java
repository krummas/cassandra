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


import java.util.*;
import java.util.concurrent.Callable;

import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies per data directory - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private final List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private final List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
    private volatile boolean enabled = true;
    public boolean isActive = true;
    private volatile CompactionParams params;
    private boolean rangeAwareCompaction;
    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private CompactionParams schemaCompactionParams;
    private Directories.DataDirectory[] locations;
    private int nextCompactionStrategy = 0;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        rangeAwareCompaction = cfs.metadata.params.compaction.rangeAwareCompaction();
        reload(cfs.metadata);
        params = cfs.metadata.params.compaction;
        locations = getDirectories().getWriteableLocations();
        enabled = params.isEnabled();

    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        maybeReload(cfs.metadata);

        assert repaired.size() == unrepaired.size();
        for (int i = 0; i < repaired.size(); i++)
        {
            int strategyIndex = (i + nextCompactionStrategy) % repaired.size();
            AbstractCompactionStrategy repStrategy = repaired.get(strategyIndex);
            AbstractCompactionStrategy unrepStrategy = unrepaired.get(strategyIndex);
            if (repStrategy.getEstimatedRemainingTasks() > unrepStrategy.getEstimatedRemainingTasks())
            {
                AbstractCompactionTask task = getTask(repStrategy, unrepStrategy, gcBefore);
                if (task != null)
                {
                    nextCompactionStrategy = strategyIndex + 1;
                    return task;
                }
            }
            else
            {
                AbstractCompactionTask task = getTask(repStrategy, unrepStrategy, gcBefore);
                if (task != null)
                {
                    nextCompactionStrategy = strategyIndex + 1;
                    return task;
                }
            }
        }
        return null;
    }

    private AbstractCompactionTask getTask(AbstractCompactionStrategy strategy1, AbstractCompactionStrategy strategy2, int gcBefore)
    {
        AbstractCompactionTask repairedTask = strategy1.getNextBackgroundTask(gcBefore);
        if (repairedTask != null)
            return repairedTask;
        return strategy2.getNextBackgroundTask(gcBefore);
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public synchronized void resume()
    {
        isActive = true;
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public synchronized void pause()
    {
        isActive = false;
    }


    private void startup()
    {
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        repaired.forEach(AbstractCompactionStrategy::startup);
        unrepaired.forEach(AbstractCompactionStrategy::startup);
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        int index = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
        if (sstable.isRepaired())
            return repaired.get(index);
        else
            return unrepaired.get(index);
    }

    public static int getCompactionStrategyIndex(ColumnFamilyStore cfs, Directories locations, SSTableReader sstable)
    {
        if (!cfs.getPartitioner().splitter().isPresent())
            return 0;

        Directories.DataDirectory[] directories = locations.getWriteableLocations();

        List<PartitionPosition> boundaries = StorageService.getDiskBoundaries(cfs, locations.getWriteableLocations());
        if (boundaries == null)
        {
            // try to figure out location based on sstable directory:
            for (int i = 0; i < directories.length; i++)
            {
                Directories.DataDirectory directory = directories[i];
                if (sstable.descriptor.directory.getAbsolutePath().startsWith(directory.location.getAbsolutePath()))
                    return i;
            }
            return 0;
        }

        int pos = Collections.binarySearch(boundaries, sstable.first);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
        return -pos - 1;


    }

    public void shutdown()
    {
        isActive = false;
        repaired.forEach(AbstractCompactionStrategy::shutdown);
        unrepaired.forEach(AbstractCompactionStrategy::shutdown);
    }

    public synchronized void maybeReload(CFMetaData metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.params.compaction.equals(schemaCompactionParams) &&
            Arrays.equals(locations, cfs.getDirectories().getWriteableLocations())) // any drives broken?
            return;
        rangeAwareCompaction = metadata.params.compaction.rangeAwareCompaction();
        reload(metadata);
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param metadata
     */
    public synchronized void reload(CFMetaData metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();
        if (!metadata.params.compaction.equals(schemaCompactionParams))
            logger.trace("Recreating compaction strategy - compaction parameters changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
        else if (!Arrays.equals(locations, cfs.getDirectories().getWriteableLocations()))
            logger.trace("Recreating compaction strategy - writeable locations changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());

        setStrategy(metadata.params.compaction);
        schemaCompactionParams = metadata.params.compaction;

        if (disabledWithJMX || !shouldBeEnabled())
            disable();
        else
            enable();
        startup();
    }

    public void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        cfs.getTracker().replaceFlushed(memtable, sstables);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(cfs);
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
            int [] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
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

    private static int[] sumArrays(int[] a, int[] b)
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

    public boolean shouldDefragment()
    {
        assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
        return repaired.get(0).shouldDefragment();
    }

    public Directories getDirectories()
    {
        assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
        return repaired.get(0).getDirectories();
    }

    public synchronized void handleNotification(INotification notification, Object sender)
    {
        maybeReload(cfs.metadata);
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            for (SSTableReader sstable : flushedNotification.added)
                getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            // a bit of gymnastics to be able to replace sstables in compaction strategies
            // we use this to know that a compaction finished and where to start the next compaction in LCS
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;

            Directories.DataDirectory [] locations = cfs.getDirectories().getWriteableLocations();
            int locationSize = cfs.getPartitioner().splitter().isPresent() ? locations.length : 1;

            List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

            for (int i = 0; i < locationSize; i++)
            {
                repairedRemoved.add(new HashSet<>());
                repairedAdded.add(new HashSet<>());
                unrepairedRemoved.add(new HashSet<>());
                unrepairedAdded.add(new HashSet<>());
            }

            for (SSTableReader sstable : listChangedNotification.removed)
            {
                int i = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
                if (sstable.isRepaired())
                    repairedRemoved.get(i).add(sstable);
                else
                    unrepairedRemoved.get(i).add(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                int i = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
                if (sstable.isRepaired())
                    repairedAdded.get(i).add(sstable);
                else
                    unrepairedAdded.get(i).add(sstable);
            }

            for (int i = 0; i < locationSize; i++)
            {
                if (!repairedRemoved.get(i).isEmpty())
                    repaired.get(i).replaceSSTables(repairedRemoved.get(i), repairedAdded.get(i));
                else
                {
                    for (SSTableReader sstable : repairedAdded.get(i))
                        repaired.get(i).addSSTable(sstable);
                }
                if (!unrepairedRemoved.get(i).isEmpty())
                    unrepaired.get(i).replaceSSTables(unrepairedRemoved.get(i), unrepairedAdded.get(i));
                else
                {
                    for (SSTableReader sstable : unrepairedAdded.get(i))
                        unrepaired.get(i).addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstable)
            {
                int index = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
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

    public void enable()
    {
        if (repaired != null)
            repaired.forEach(AbstractCompactionStrategy::enable);
        if (unrepaired != null)
            unrepaired.forEach(AbstractCompactionStrategy::enable);
        // enable this last to make sure the strategies are ready to get calls.
        enabled = true;
    }

    public void disable()
    {
        // disable this first avoid asking disabled strategies for compaction tasks
        enabled = false;
        if (repaired != null)
            repaired.forEach(AbstractCompactionStrategy::disable);
        if (unrepaired != null)
            unrepaired.forEach(AbstractCompactionStrategy::disable);
    }

    /**
     * Create ISSTableScanners from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param ranges
     * @return
     */
    @SuppressWarnings("resource")
    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        assert repaired.size() == unrepaired.size();
        List<Set<SSTableReader>> repairedSSTables = new ArrayList<>();
        List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>();

        for (int i = 0; i < repaired.size(); i++)
        {
            repairedSSTables.add(new HashSet<>());
            unrepairedSSTables.add(new HashSet<>());
        }

        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired())
                repairedSSTables.get(getCompactionStrategyIndex(cfs, getDirectories(), sstable)).add(sstable);
            else
                unrepairedSSTables.get(getCompactionStrategyIndex(cfs, getDirectories(), sstable)).add(sstable);
        }

        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());

        for (Range<Token> range : ranges)
        {
            List<ISSTableScanner> repairedScanners = new ArrayList<>();
            List<ISSTableScanner> unrepairedScanners = new ArrayList<>();

            for (int i = 0; i < repairedSSTables.size(); i++)
            {
                if (!repairedSSTables.get(i).isEmpty())
                    repairedScanners.addAll(repaired.get(i).getScanners(repairedSSTables.get(i), range).scanners);
            }
            for (int i = 0; i < unrepairedSSTables.size(); i++)
            {
                if (!unrepairedSSTables.get(i).isEmpty())
                    scanners.addAll(unrepaired.get(i).getScanners(unrepairedSSTables.get(i), range).scanners);
            }
            for (ISSTableScanner scanner : Iterables.concat(repairedScanners, unrepairedScanners))
            {
                if (!scanners.add(scanner))
                    scanner.close();
            }
        }
        return new AbstractCompactionStrategy.ScannerList(scanners);
    }

    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, Collections.singleton(null));
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        // todo:
        return unrepaired.get(0).groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    public long getMaxSSTableBytes()
    {
        return unrepaired.get(0).getMaxSSTableBytes();
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        maybeReload(cfs.metadata);
        validateForCompaction(txn.originals());
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        SSTableReader firstSSTable = Iterables.getFirst(input, null);
        assert firstSSTable != null;
        boolean repaired = firstSSTable.isRepaired();
        int firstIndex = getCompactionStrategyIndex(cfs, getDirectories(), firstSSTable);
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (firstIndex != getCompactionStrategyIndex(cfs, getDirectories(), sstable))
                throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
        }
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        maybeReload(cfs.metadata);
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
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired)
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    if (tasks.isEmpty())
                        return null;
                    return tasks;
                }
            }
        }, false, false);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        maybeReload(cfs.metadata);
        validateForCompaction(sstables);
        return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
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
        return params.isEnabled();
    }

    public String getName()
    {
        return unrepaired.get(0).getName();
    }

    public List<List<AbstractCompactionStrategy>> getStrategies()
    {
        return Arrays.asList(repaired, unrepaired);
    }

    public synchronized void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        setStrategy(params);
        if (shouldBeEnabled())
            enable();
        else
            disable();
        startup();
    }

    private void setStrategy(CompactionParams params)
    {
        repaired.forEach(AbstractCompactionStrategy::shutdown);
        unrepaired.forEach(AbstractCompactionStrategy::shutdown);
        repaired.clear();
        unrepaired.clear();

        if (cfs.getPartitioner().splitter().isPresent())
        {
            locations = cfs.getDirectories().getWriteableLocations();
            for (int i = 0; i < locations.length; i++)
            {
                if (rangeAwareCompaction)
                {
                    repaired.add(new VNodeAwareCompactionStrategy(cfs, params));
                    unrepaired.add(new VNodeAwareCompactionStrategy(cfs, params));
                }
                else
                {
                    repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                    unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                }
            }
        }
        else
        {
            repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
            unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
        }
        this.params = params;
    }

    public CompactionParams getCompactionParams()
    {
        return params;
    }

    public boolean onlyPurgeRepairedTombstones()
    {
        return Boolean.parseBoolean(params.options().get(AbstractCompactionStrategy.ONLY_PURGE_REPAIRED_TOMBSTONES));
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, MetadataCollector collector, SerializationHeader header, LifecycleTransaction txn)
    {
        if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            return unrepaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, txn);
        else
            return repaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, txn);
    }
}
