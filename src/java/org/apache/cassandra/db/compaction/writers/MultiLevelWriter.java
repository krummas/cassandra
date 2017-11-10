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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

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
    private Directories.DataDirectory sstableDirectory;
    private final List<Pair<Bounds<PartitionPosition>, Integer>> rangeLevels;
    private int level = 0;
    private int currentRange = 0;

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
        this.rangeLevels = calculateRangeLevels(nonExpiredSSTables, boundaries != null ? new Bounds<>(boundaries.left.minKeyBound(), boundaries.right.maxKeyBound()) : null);
        // todo: based on the rangeLevels, calculate sstable size for L0 correctly
        long totalSize = getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        estimatedSSTables = Math.max(1, totalSize / maxSSTableSize);
    }

    /**
     * Calculate the level for all the ranges we are compacting. Boundaries are based on sstable first/last tokens
     *
     * figures out the max/min tokens in each level, then makes sure that we construct the ranges so that
     * as much data as possible is pushed to the highest possible level without causing overlap
     *
     */
    @VisibleForTesting
    static List<Pair<Bounds<PartitionPosition>, Integer>> calculateRangeLevels(Iterable<SSTableReader> sstables, Bounds<PartitionPosition> originalBoundaries)
    {
        Token [] perLevelMinTokens = new Token[LeveledManifest.MAX_LEVEL_COUNT + 1];
        Token [] perLevelMaxTokens = new Token[LeveledManifest.MAX_LEVEL_COUNT + 1];
        int maxLevel = 0;
        Token minToken = null, maxToken = null;
        for (SSTableReader sstable : sstables)
        {
            int level = sstable.getSSTableLevel();
            if (perLevelMinTokens[level] == null || perLevelMinTokens[level].compareTo(sstable.first.getToken()) > 0)
                perLevelMinTokens[level] = sstable.first.getToken();
            if (perLevelMaxTokens[level] == null || perLevelMaxTokens[level].compareTo(sstable.last.getToken()) < 0)
                perLevelMaxTokens[level] = sstable.last.getToken();
            maxLevel = Math.max(maxLevel, level);
            if (maxToken == null || maxToken.compareTo(sstable.last.getToken()) < 0)
                maxToken = sstable.last.getToken();
            if (minToken == null || minToken.compareTo(sstable.first.getToken()) > 0)
                minToken = sstable.first.getToken();
        }
        assert minToken != null && maxToken != null;
        Bounds<PartitionPosition> origBounds = originalBoundaries == null ? new Bounds<>(minToken.minKeyBound(), maxToken.maxKeyBound()) : originalBoundaries;
        List<Bounds<PartitionPosition>> perLevelBounds = new ArrayList<>(maxLevel + 1);
        for (int i = 0; i < maxLevel + 1; i++)
        {
            if (perLevelMaxTokens[i] == null)
            {
                perLevelBounds.add(origBounds);
                continue;
            }
            // we need to make sure we always push at least the original boundaries to the topmost level
            PartitionPosition min = perLevelMinTokens[i].minKeyBound().compareTo(origBounds.left) < 0 ? perLevelMinTokens[i].minKeyBound() : origBounds.left;
            PartitionPosition max = perLevelMaxTokens[i].maxKeyBound().compareTo(origBounds.right) > 0 ? perLevelMaxTokens[i].maxKeyBound() : origBounds.right;
            perLevelBounds.add(new Bounds<>(min, max));
        }
        return getLevelBounds(perLevelBounds);
    }

    @VisibleForTesting
    static List<Pair<Bounds<PartitionPosition>, Integer>> getLevelBounds(List<Bounds<PartitionPosition>> perLevelBounds)
    {
        int maxLevel = perLevelBounds.size() - 1;
        Token first = null, last = null;
        for (Bounds<PartitionPosition> bound : perLevelBounds)
        {
            if (first == null || bound.left.getToken().compareTo(first) < 0)
                first = bound.left.getToken();
            if (last == null || bound.right.getToken().compareTo(last) > 0)
                last = bound.right.getToken();
        }
        System.out.println("first "+first+" -> last "+last);
        List<Pair<Bounds<PartitionPosition>, Integer>> bounds = new ArrayList<>();
        for (int i = maxLevel; i >= 0; i--)
        {
            List<Bounds<PartitionPosition>> levelBounds = new ArrayList<>();
            levelBounds.add(perLevelBounds.get(i));
            System.out.println("level = "+i);
            for (int j = maxLevel; j > i; j--)
            {
                List<Bounds<PartitionPosition>> newLevelBounds = new ArrayList<>();
                for (Bounds<PartitionPosition> b : levelBounds)
                {
                    System.out.println("subtract "+b+" "+perLevelBounds.get(j) + " = "+subtract(b, perLevelBounds.get(j)));
                    newLevelBounds.addAll(subtract(b, perLevelBounds.get(j)));
                }
                levelBounds = newLevelBounds;
            }
            for (Bounds<PartitionPosition> b : levelBounds)
                bounds.add(Pair.create(b, i));
        }
        bounds.sort(Comparator.comparing(o -> o.left.left));

        // make sure the entire range is covered
        assert first != null;
        assert last != null;
        PartitionPosition start = first.minKeyBound();
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
        assert start.getToken().equals(last);
        assert start.kind() == MAX_BOUND;
        return bounds;
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
        if (leftCompareRight < 0 && rightCompareLeft > 0)
            return true;
        if (leftCompareRight == 0 && lhs.left.kind() == MIN_BOUND && rhs.right.kind() == MAX_BOUND)
            return true;
        if (rightCompareLeft == 0 && lhs.right.kind() == MAX_BOUND && rhs.left.kind() == MIN_BOUND)
            return true;

        return false;
    }

    private static boolean contains(Bounds<PartitionPosition> bounds, PartitionPosition pos)
    {
        int compareLeft = pos.compareTo(bounds.left);
        int compareRight = pos.compareTo(bounds.right);
        if (compareLeft > 0 && compareRight < 0 ||
            compareLeft == 0 && bounds.left.kind() == MIN_BOUND && pos.kind() == MAX_BOUND ||
            compareRight == 0 && bounds.right.kind() == MAX_BOUND && pos.kind() == MIN_BOUND)
            return true;

        return false;
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
        int newLevel = getLevelFor(partition.partitionKey());
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

    private int getLevelFor(DecoratedKey key)
    {
        while (currentRange < rangeLevels.size() && !rangeLevels.get(currentRange).left.contains(key))
            currentRange++;
        if (currentRange == rangeLevels.size())
            throw new RuntimeException("All tokens should be in a range!");
        return rangeLevels.get(currentRange).right;
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
}
