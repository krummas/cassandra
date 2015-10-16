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

import java.io.File;
import java.util.*;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class VNodeAwareCompactionStrategy extends AbstractCompactionStrategy
{
    public static final String MIN_VNODE_SSTABLE_SIZE_MB = "min_vnode_sstable_size_in_mb";

    private static final Logger logger = LoggerFactory.getLogger(VNodeAwareCompactionStrategy.class);
    private final Set<SSTableReader> l0sstables = new HashSet<>();
    private final List<AbstractCompactionStrategy> strategies = new ArrayList<>();
    private List<Token> vnodeBoundaries = null;
    private final Set<SSTableReader> unknownSSTables = new HashSet<>();
    private final long vnodeSSTableSize;
    private final SizeTieredCompactionStrategyOptions sizeTieredOptions;
    private List<Range<Token>> localRanges;
    private int estimatedL0compactions = 0;
    private CompactionParams compactionParams;
    private int nextVNodeToCompact = 0;

    public VNodeAwareCompactionStrategy(ColumnFamilyStore cfs, CompactionParams params)
    {
        super(cfs, params.options());

        if (options.containsKey(MIN_VNODE_SSTABLE_SIZE_MB))
            vnodeSSTableSize = Long.parseLong(options.get(MIN_VNODE_SSTABLE_SIZE_MB)) * (1L << 20);
        else
            vnodeSSTableSize = 10_000_000;
        compactionParams = params;
        sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    private void refreshVnodeBoundaries()
    {
        Collection<Range<Token>> lr = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
        if (lr == null || lr.isEmpty())
        {
            logger.warn("Don't know the local ranges yet, not doing any compaction.");
            return;
        }
        List<Range<Token>> newLocalRanges = Range.sort(lr);

        if (localRanges != null && newLocalRanges.equals(localRanges))
        {
            logger.trace("No change in local ranges");
            return;
        }

        localRanges = Range.sort(lr);

        vnodeBoundaries = new ArrayList<>();
        for (Range<Token> r : newLocalRanges)
            vnodeBoundaries.add(r.right == cfs.getPartitioner().getMinimumToken() ? cfs.getPartitioner().getMaximumToken() : r.right);
        vnodeBoundaries.set(vnodeBoundaries.size() - 1, cfs.getPartitioner().getMaximumToken());
        for (int i = 0; i < vnodeBoundaries.size(); i++)
            strategies.add(CFMetaData.createCompactionStrategyInstance(cfs, compactionParams));
        // unknown sstables are the ones added before we knew the vnode boundaries:
        unknownSSTables.forEach(this::addSSTable);

        unknownSSTables.clear();
    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        refreshVnodeBoundaries();
        if (vnodeBoundaries == null || vnodeBoundaries.isEmpty())
            return null;
        // need to get both l0 and l1 before checking which to return in order to update estimatedRemainingCompactions
        Iterable<SSTableReader> l0candidates = getL0Candidates();

        if (l0candidates != null)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(l0candidates, OperationType.COMPACTION);
            if (txn != null)
                return new VNodeAwareCompactionTask(cfs, txn, gcBefore, vnodeBoundaries, localRanges, vnodeSSTableSize);
        }

        return getL1Candidates(gcBefore);
    }

    private Iterable<SSTableReader> getL0Candidates()
    {
        Iterable<SSTableReader> candidates = cfs.getTracker().getUncompacting(l0sstables);
        List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    sizeTieredOptions.bucketHigh,
                                                                                    sizeTieredOptions.bucketLow,
                                                                                    sizeTieredOptions.minSSTableSize);
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();
        estimatedL0compactions = SizeTieredCompactionStrategy.getEstimatedCompactionsByTasks(cfs, buckets);
        Iterable<SSTableReader> interesting = SizeTieredCompactionStrategy.mostInterestingBucket(buckets, minThreshold, maxThreshold);


        if (interesting != null && !Iterables.isEmpty(interesting))
            return interesting;

        return null;
    }

    private AbstractCompactionTask getL1Candidates(int gcBefore)
    {
        for (int i = 0; i < strategies.size(); i++)
        {
            int toCompactIndex = (i + nextVNodeToCompact) % strategies.size();
            AbstractCompactionTask task = strategies.get(toCompactIndex).getNextBackgroundTask(gcBefore);

            if (task != null)
            {
                nextVNodeToCompact = (toCompactIndex + 1) % strategies.size();
                return task;
            }
        }
        return null;
    }


    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        return null;
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return null;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        int remaining = 0;
        for (AbstractCompactionStrategy strategy : strategies)
            remaining += strategy.getEstimatedRemainingTasks();
        return estimatedL0compactions + remaining;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        if (vnodeBoundaries == null || vnodeBoundaries.isEmpty())
        {
            unknownSSTables.add(added);
            return;
        }

        if (!isSingleVNode(added))
            l0sstables.add(added);
        else
            getSSTablesFor(added.first.getToken()).addSSTable(added);
    }

    private boolean isSingleVNode(SSTableReader added)
    {
        int vnodeIndex = getVNodeIndex(added.first.getToken());
        if (vnodeIndex == vnodeBoundaries.size() - 1)
            return true; // the last boundary contains everything to maxtoken
        return vnodeBoundaries.get(vnodeIndex + 1).compareTo(added.last.getToken()) > 0;
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        unknownSSTables.remove(sstable);
        if (vnodeBoundaries == null || vnodeBoundaries.isEmpty())
            return;

        if (!isSingleVNode(sstable))
            l0sstables.remove(sstable);
        else
            getSSTablesFor(sstable.first.getToken()).removeSSTable(sstable);
    }

    private AbstractCompactionStrategy getSSTablesFor(Token token)
    {
        return strategies.get(getVNodeIndex(token));
    }

    private int getVNodeIndex(Token token)
    {
        int pos = Collections.binarySearch(vnodeBoundaries, token);
        if (pos < 0)
            pos = -pos - 1;
        return pos;
    }

    private static class VNodeAwareCompactionTask extends CompactionTask
    {
        private final List<Token> vnodeBoundaries;
        private final List<Range<Token>> localRanges;
        private final long vnodeSSTableSize;

        public VNodeAwareCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, List<Token> vnodeBoundaries, List<Range<Token>> localRanges, long vnodeSSTableSize)
        {
            super(cfs, txn, gcBefore, false, false);
            this.vnodeBoundaries = vnodeBoundaries;
            this.localRanges = localRanges;
            this.vnodeSSTableSize = vnodeSSTableSize;
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction transaction,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new VNodeAwareCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, vnodeBoundaries, localRanges, vnodeSSTableSize);
        }
    }

    public static class VNodeAwareCompactionWriter extends CompactionAwareWriter
    {
        private final SSTableRewriter l1writer;
        private final List<Token> vnodeBoundaries;
        private final double compactionGain;
        private final long vnodeSSTableSize;
        private SSTableRewriter activeWriter;
        private int vnodeIndex = -1;
        private Directories.DataDirectory location;
        private long lastEstimate = 0;
        private long lastStart = 0;

        public VNodeAwareCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, List<Token> vnodeBoundaries, List<Range<Token>> localRanges, long vnodeSSTableSize)
        {
            // note that we cant open early here as we are writing to two different files
            super(cfs, directories, txn, nonExpiredSSTables, false, false, false);
            txn.permitRedundantTransitions(); // we are writing to several files
            l1writer = new SSTableRewriter(txn, maxAge, false, false);
            this.vnodeBoundaries = vnodeBoundaries;
            compactionGain = SSTableReader.estimateCompactionGain(nonExpiredSSTables);
            this.vnodeSSTableSize = vnodeSSTableSize;
        }

        @Override
        public boolean realAppend(UnfilteredRowIterator partition)
        {
            maybeSwitchVNode(partition.partitionKey());
            activeWriter.append(partition);
            return false;
        }

        @Override
        public void switchCompactionLocation(Directories.DataDirectory location)
        {
            this.location = location;
            logger.debug("Switching compaction location to {}", location.location);
            // note that we only switch the l0 writer if we switch compaction location
            sstableWriter.switchWriter(createWriter(location, estimatedTotalKeys, 0));
            if (l1writer.currentWriter() != null)
                l1writer.switchWriter(createWriter(location, estimatedTotalKeys, 0));
        }

        private void maybeSwitchVNode(DecoratedKey key)
        {
            if (vnodeIndex > -1 && key.getToken().compareTo(vnodeBoundaries.get(vnodeIndex)) < 0)
                return;

            while (vnodeIndex == -1 || key.getToken().compareTo(vnodeBoundaries.get(vnodeIndex)) >= 0)
                vnodeIndex++;

            Token vnodeStart = vnodeIndex == 0 ? cfs.getPartitioner().getMinimumToken() : vnodeBoundaries.get(vnodeIndex - 1);
            Token vnodeEnd = vnodeIndex == vnodeBoundaries.size() ? cfs.getPartitioner().getMaximumToken() : vnodeBoundaries.get(vnodeIndex);
            long estimatedVNodeSize = estimateVNodeSize(nonExpiredSSTables, new Range<>(vnodeStart, vnodeEnd));
            if (logger.isTraceEnabled() && lastEstimate != 0 && activeWriter != null)
            {
                long lastWriteSize = activeWriter.currentWriter().getOnDiskFilePointer() - lastStart;
                logger.trace("Last estimate was: {} - actual write size was: {} ({})", lastEstimate, lastWriteSize, (double)lastWriteSize/lastEstimate);
            }
            lastEstimate = estimatedVNodeSize;
            if (estimatedVNodeSize >= vnodeSSTableSize)
            {
                l1writer.switchWriter(createWriter(location, estimatedTotalKeys, 1)); // todo: better estimate of total keys
                logger.debug("writing vnode {} to L1, estimated size = {}, loc = {}", vnodeIndex, estimatedVNodeSize, l1writer.currentWriter().getFilename());

                activeWriter = l1writer;
            }
            else
            {
                activeWriter = sstableWriter;
                logger.debug("writing vnode {} to L0, estimated size = {}, loc = {}", vnodeIndex, estimatedVNodeSize, sstableWriter.currentWriter().getFilename());
            }
            lastStart = activeWriter.currentWriter().getOnDiskFilePointer();
        }

        private long estimateVNodeSize(Set<SSTableReader> sstables, Range<Token> tokenRange)
        {
            long sum = 0;
            for (SSTableReader sstable : sstables)
            {
                List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(Collections.singletonList(tokenRange));
                for (Pair<Long, Long> pos : positions)
                    sum += pos.right - pos.left;
            }
            double compressionRatio = cfs.metric.compressionRatio.getValue();
            double uncompressedSize = compactionGain * sum;
            return Math.round((compressionRatio > 0d) ? compressionRatio * uncompressedSize : uncompressedSize);
        }

        @Override
        public Throwable doAbort(Throwable accumulate)
        {
            activeWriter = null;
            accumulate = super.doAbort(accumulate);
            return l1writer.abort(accumulate);
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = super.doCommit(accumulate);
            return l1writer.commit(accumulate);
        }

        @Override
        protected void doPrepare()
        {
            super.doPrepare();
            l1writer.prepareToCommit();
        }

        @Override
        public List<SSTableReader> finish()
        {
            List<SSTableReader> finished = new ArrayList<>();
            finished.addAll(super.finish());
            finished.addAll(l1writer.finished());
            return finished;
        }

        @Override
        public List<SSTableReader> finish(long repairedAt)
        {
            List<SSTableReader> finished = new ArrayList<>();
            finished.addAll(sstableWriter.setRepairedAt(repairedAt).finish());
            finished.addAll(l1writer.setRepairedAt(repairedAt).finish());
            return finished;
        }

        private SSTableWriter createWriter(Directories.DataDirectory directory, long estimatedKeys, int level)
        {
            File sstableDirectory = getDirectories().getLocationForDisk(directory);
            return SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(sstableDirectory)),
                    estimatedKeys,
                    minRepairedAt,
                    cfs.metadata,
                    new MetadataCollector(txn.originals(), cfs.metadata.comparator, level),
                    SerializationHeader.make(cfs.metadata, txn.originals()),
                    txn);
        }
    }



    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String size = options.containsKey(MIN_VNODE_SSTABLE_SIZE_MB) ? options.get(MIN_VNODE_SSTABLE_SIZE_MB) : "1";
        try
        {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be >= 0, but was %s", MIN_VNODE_SSTABLE_SIZE_MB, ssSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, MIN_VNODE_SSTABLE_SIZE_MB), ex);
        }

        uncheckedOptions.remove(MIN_VNODE_SSTABLE_SIZE_MB);

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }
}
