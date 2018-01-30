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
package org.apache.cassandra.db.compaction.writers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.PartitionPosition.Kind.MAX_BOUND;
import static org.apache.cassandra.db.PartitionPosition.Kind.MIN_BOUND;

public class MultiLevelWriter extends CompactionAwareWriter
{
    private final long maxSSTableSize;
    private final long estimatedSSTables;
    private final Set<SSTableReader> allSSTables;
    private final RangeLevels rangeLevels;
    private Directories.DataDirectory sstableDirectory;
    private int level = 0;

    public MultiLevelWriter(ColumnFamilyStore cfs,
                            Directories directories,
                            LifecycleTransaction txn,
                            Set<SSTableReader> sstables,
                            Range<Token> boundaries,
                            long maxSSTableSize,
                            boolean keepOriginals)
    {
        super(cfs, directories, txn, sstables, keepOriginals);
        this.allSSTables = txn.originals();
        this.maxSSTableSize = maxSSTableSize;
        this.rangeLevels = new RangeLevels(nonExpiredSSTables, boundaries);
        // todo: based on the rangeLevels, calculate sstable size for L0 correctly
        long totalSize = getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        estimatedSSTables = Math.max(1, totalSize / maxSSTableSize);
    }

    /**
     * Gets the estimated total amount of data to write during compaction
     */
    private static long getTotalWriteSize(Iterable<SSTableReader> nonExpiredSSTables, long estimatedTotalKeys, ColumnFamilyStore cfs, OperationType compactionType)
    {
        long estimatedKeysBeforeCompaction = 0;
        for (SSTableReader sstable : nonExpiredSSTables)
            estimatedKeysBeforeCompaction += sstable.estimatedKeys();
        estimatedKeysBeforeCompaction = Math.max(1, estimatedKeysBeforeCompaction);
        double estimatedCompactionRatio = (double) estimatedTotalKeys / estimatedKeysBeforeCompaction;

        return Math.round(estimatedCompactionRatio * cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
    }

    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        int newLevel = rangeLevels.getLevelFor(partition.partitionKey());
        if (newLevel != level)
        {
            level = newLevel;
            switchCompactionLocation(sstableDirectory);
        }
        RowIndexEntry rie = sstableWriter.append(partition);
        // for L0 we want to make create 'big' files - otherwise we might start triggering STCS in L0 compactions once
        // we are done:
        if (level > 0 && sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > maxSSTableSize)
        {
            switchCompactionLocation(sstableDirectory);
        }
        return rie != null;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        sstableDirectory = location;
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                                                    estimatedTotalKeys / estimatedSSTables,
                                                    minRepairedAt,
                                                    pendingRepair,
                                                    cfs.metadata,
                                                    new MetadataCollector(allSSTables, cfs.metadata().comparator, level),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);

        sstableWriter.switchWriter(writer);
    }

    @VisibleForTesting
    static class RangeLevels
    {
        private final Set<SSTableReader> sstables;
        final ImmutableList<Pair<Bounds<PartitionPosition>, Integer>> rangeLevels;
        private int currentRange = 0;

        RangeLevels(Set<SSTableReader> nonExpiredSSTables, Range<Token> boundaries)
        {
            sstables = nonExpiredSSTables;
            rangeLevels = calculate(new Bounds<>(boundaries.left.minKeyBound(), boundaries.right.maxKeyBound()));
        }

        int getLevelFor(DecoratedKey key)
        {
            while (currentRange < rangeLevels.size() && !rangeLevels.get(currentRange).left.contains(key))
                currentRange++;
            if (currentRange == rangeLevels.size())
                throw new RuntimeException("All tokens should be in a range!");
            return rangeLevels.get(currentRange).right;
        }

        /**
         * Calculate the level for all the ranges we are compacting. Boundaries are based on sstable first/last tokens
         *
         * figures out the max/min tokens in each level, then makes sure that we construct the ranges so that
         * as much data as possible is pushed to the highest possible level without causing overlap
         *
         */
        private ImmutableList<Pair<Bounds<PartitionPosition>, Integer>> calculate(Bounds<PartitionPosition> originalBoundaries)
        {
            Levels levels = new Levels(originalBoundaries);
            for (SSTableReader sstable : sstables)
                levels.add(sstable);

            List<Pair<Bounds<PartitionPosition>, Integer>> bounds = new ArrayList<>();
            for (int i = levels.getMaxLevel(); i >= 0; i--)
            {
                List<Bounds<PartitionPosition>> levelBounds = new ArrayList<>();
                levelBounds.add(levels.getBoundariesForLevel(i));
                // now we need to subtract the boundaries of all levels higher than the one we just added:
                for (int j = levels.getMaxLevel(); j > i; j--)
                {
                    List<Bounds<PartitionPosition>> newLevelBounds = new ArrayList<>();
                    for (Bounds<PartitionPosition> b : levelBounds)
                        newLevelBounds.addAll(subtract(b, levels.getBoundariesForLevel(j)));

                    levelBounds = newLevelBounds;
                }
                for (Bounds<PartitionPosition> b : levelBounds)
                    bounds.add(Pair.create(b, i));
            }
            bounds.sort(Comparator.comparing(o -> o.left.left));

            // make sure the entire range is covered
            PartitionPosition start = levels.minBoundary();
            // the boundaries should be laid out something like this:
            // [   )[   )[   ](   ](   ]
            // lvl:   0    1    2    1    0
            for (Pair<Bounds<PartitionPosition>, Integer> p : bounds)
            {
                Bounds<PartitionPosition> b = p.left;
                assert start.getToken().equals(b.left.getToken());
                assert start.kind() == b.left.kind();
                start = b.right;
            }
            assert start.getToken().maxKeyBound().equals(levels.maxBoundary()) : start.getToken().maxKeyBound() + " != " + levels.maxBoundary();
            assert start.kind() == MAX_BOUND;
            return ImmutableList.copyOf(bounds);
        }
    }

    @VisibleForTesting
    static class Levels
    {
        // index in these arrays is the level, perLevelMinTokens keeps the smallest token for the level, perLevelMaxTokens the biggest
        private final Token [] perLevelMinTokens = new Token[LeveledManifest.MAX_LEVEL_COUNT + 1];
        private final Token [] perLevelMaxTokens = new Token[LeveledManifest.MAX_LEVEL_COUNT + 1];
        private final Bounds<PartitionPosition> boundaries;
        private int maxLevel;
        private Token maxToken;
        private Token minToken;

        public Levels(Bounds<PartitionPosition> originalBoundaries)
        {
            this.boundaries = originalBoundaries;
        }

        public void add(SSTableReader sstable)
        {
            int level = sstable.getSSTableLevel();
            addToken(level, sstable.first.getToken());
            addToken(level, sstable.last.getToken());
        }

        public void addToken(int level, Token t)
        {
            if (perLevelMinTokens[level] == null || perLevelMinTokens[level].compareTo(t) > 0)
                perLevelMinTokens[level] = t;
            if (perLevelMaxTokens[level] == null || perLevelMaxTokens[level].compareTo(t) < 0)
                perLevelMaxTokens[level] = t;
            maxLevel = Math.max(maxLevel, level);
            if (maxToken == null || maxToken.compareTo(t) < 0)
                maxToken = t;
            if (minToken == null || minToken.compareTo(t) > 0)
                minToken = t;
        }

        public int getMaxLevel()
        {
            return maxLevel;
        }
        /**
         * Get the boundaries for the level
         *
         * this is either the smallest/largest token for sstables from the level or the given boundary.
         *
         * For example, if we request compaction for boundaries [100, 200] but the sstables in the only cover
         * [140, 150] it means that [100, 140) and (150, 200] are empty in the level and we can add tokens from lower
         * levels without causing overlap.
         */
        public Bounds<PartitionPosition> getBoundariesForLevel(int level)
        {
            PartitionPosition min = minBoundForLevel(level);
            PartitionPosition max = maxBoundForLevel(level);
            return new Bounds<>(min, max);
        }

        private PartitionPosition minBoundForLevel(int level)
        {
            if (perLevelMinTokens[level] == null)
                return boundaries.left;
            return perLevelMinTokens[level].minKeyBound().compareTo(boundaries.left) < 0 ? perLevelMinTokens[level].minKeyBound() : boundaries.left;
        }

        private PartitionPosition maxBoundForLevel(int level)
        {
            if (perLevelMaxTokens[level] == null)
                return boundaries.right;
            return perLevelMaxTokens[level].maxKeyBound().compareTo(boundaries.right) > 0 ? perLevelMaxTokens[level].maxKeyBound() : boundaries.right;
        }

        /**
         * global min boundary for the sstables/levels involved
         */
        public PartitionPosition minBoundary()
        {
            if (boundaries.left.compareTo(minToken.minKeyBound()) < 0)
                return boundaries.left;
            return minToken.minKeyBound();
        }

        /**
         * global max boundary for the sstables/levels involved
         */
        public PartitionPosition maxBoundary()
        {
            if (boundaries.right.compareTo(maxToken.maxKeyBound()) > 0)
                return boundaries.right;
            return maxToken.maxKeyBound();
        }
    }

    @VisibleForTesting
    static Collection<Bounds<PartitionPosition>> subtract(Bounds<PartitionPosition> lhs, Bounds<PartitionPosition> rhs)
    {
        if (!intersects(lhs, rhs))
            return Collections.singletonList(lhs);

        List<Bounds<PartitionPosition>> res = new ArrayList<>(2);
        if (contains(lhs, rhs.left))
            res.add(new Bounds<>(lhs.left, rhs.left));
        if (contains(lhs, rhs.right))
            res.add(new Bounds<>(rhs.right, lhs.right));
        return res;
    }

    private static boolean intersects(Bounds<PartitionPosition> lhs, Bounds<PartitionPosition> rhs)
    {
        int leftCompareRight = lhs.left.compareTo(rhs.right);
        int rightCompareLeft = lhs.right.compareTo(rhs.left);
        return leftCompareRight < 0 && rightCompareLeft > 0 ||
               leftCompareRight == 0 && lhs.left.kind() == MIN_BOUND && rhs.right.kind() == MAX_BOUND ||
               rightCompareLeft == 0 && lhs.right.kind() == MAX_BOUND && rhs.left.kind() == MIN_BOUND;
    }

    private static boolean contains(Bounds<PartitionPosition> bounds, PartitionPosition pos)
    {
        int compareLeft = pos.compareTo(bounds.left);
        int compareRight = pos.compareTo(bounds.right);
        return compareLeft > 0 && compareRight < 0 ||
               compareLeft == 0 && bounds.left.kind() == MIN_BOUND && pos.kind() == MAX_BOUND ||
               compareRight == 0 && bounds.right.kind() == MAX_BOUND && pos.kind() == MIN_BOUND;
    }
}
