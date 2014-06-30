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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    public static final int MAX_COMPACTING_L0 = 32;
    private static final int NO_COMPACTION_LIMIT = 25;
    public static final int LEVEL_COUNT = (int) Math.log10(1000 * 1000 * 1000);

    private final ColumnFamilyStore cfs;
    @VisibleForTesting
    protected final List<SSTableReader>[] generations;

    // last compacted key tracks what key we last compacted per level
    // so that we don't keep compacting the same tokens
    private final RowPosition[] lastCompactedKeys;
    private final int maxSSTableSizeInBytes;
    private Boolean hasRepairedData = null;
    private final int [] compactionCounter;

    public LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024 * 1024;

        generations = new List[LEVEL_COUNT];
        lastCompactedKeys = new RowPosition[LEVEL_COUNT];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            lastCompactedKeys[i] = cfs.partitioner.getMinimumToken().minKeyBound();
        }
        compactionCounter = new int[LEVEL_COUNT];
    }

    public void add(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level > 0 : "Manifest should only contain sstables with level > 0";
        assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
        logDistribution();
        logger.debug("Adding {} to L{}", reader, level);
        generations[level].add(reader);
        if (hasRepairedData == null)
            hasRepairedData = reader.isRepaired();
        else if (!hasRepairedData.equals(reader.isRepaired()))
            throw new AssertionError("We should never mix repaired and unrepaired data in the leveled manifest.");
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    public boolean canAddSSTable(SSTableReader sstable)
    {
        int level = sstable.getSSTableLevel();
        assert level > 0;

        // when we have repaired data in the manifest, we will not have any unrepaired data
        // - it should go into L0 in LMW
        if (hasRepairedData != null && !sstable.isRepaired() && hasRepairedData)
            return false;

        List<SSTableReader> copyLevel = new ArrayList<>(generations[level]);
        copyLevel.add(sstable);
        Collections.sort(copyLevel, SSTableReader.sstableComparator);

        SSTableReader previous = null;
        for (SSTableReader current : copyLevel)
        {
            if (previous != null && current.originalFirst.compareTo(previous.last) <= 0)
                return false;
            previous = current;
        }
        return true;
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    @VisibleForTesting
    long maxBytesForLevel(int level)
    {
        // L1 should have one sstable, L2 10 etc:
        double bytes = Math.pow(10, level - 1) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public CompactionCandidate getCompactionCandidates()
    {
        // todo: update comment
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.
        for (int i = generations.length - 1; i > 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
            double score = (double) SSTableReader.getTotalBytes(remaining) / (double)maxBytesForLevel(i);
            logger.debug("Compaction score for level {} is {}", i, score);

            if (score > 1.001)
            {
                // stcs in l0 is handled outside the manifest
                Collection<SSTableReader> candidates = getCandidatesFor(i);
                if (!candidates.isEmpty())
                {
                    int nextLevel = getNextLevel(candidates);
                    candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                    if (logger.isDebugEnabled())
                        logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
                    return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
                }
                else
                {
                    logger.debug("No compaction candidates for L{}", i);
                }
            }
        }
        return null;
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isDebugEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.debug("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    RowPosition max = null;
                    RowPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
                    Range<RowPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<RowPosition> r = new Range<RowPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
        return getLevel(i).size();
    }

    public int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.debug("L{} contains {} SSTables ({} bytes) in {}",
                                 i, getLevel(i).size(), SSTableReader.getTotalBytes(getLevel(i)), this);
                }
            }
        }
    }

    @VisibleForTesting
    public int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level > 0 : reader + " not present in manifest: "+level;
        boolean removed = generations[level].remove(reader);
        if (!removed)
        {
            logger.warn("SSTable {} (level = {}, rep = ({}, {}), {} -> {}) not found in manifest", reader, reader.getSSTableLevel(), reader.isRepaired(), hasRepairedData, reader.originalFirst, reader.last);
            throw new AssertionError("Could not remove sstable");
        }
        return level;
    }

    @VisibleForTesting
    static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        return overlapping(sstable.originalFirst.getToken(), sstable.last.getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<SSTableReader>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<Token>(candidate.originalFirst.getToken(), candidate.last.getToken());
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesFor(int level)
    {
        assert !getLevel(level).isEmpty();
        logger.debug("Choosing candidates for L{}", level);

        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();

        // for non-L0 compactions, pick up where we left off last time
        Collections.sort(getLevel(level), SSTableReader.sstableComparator);
        int start = 0; // handles case where the prior compaction touched the very last range
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get(i);
            if (sstable.originalFirst.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }

        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get((start + i) % getLevel(level).size());
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlapping(sstable, getLevel(level + 1)));
            if (Iterables.any(candidates, suspectP))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (getLevel(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i > 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            estimated[i] = Math.max(0L, SSTableReader.getTotalBytes(sstables) - maxBytesForLevel(i)) / maxSSTableSizeInBytes;
            tasks += estimated[i];
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;

    }

    public boolean hasRepairedData()
    {
        if (hasRepairedData == null)
            return false;
        return hasRepairedData;
    }

    /**
     * Drains all sstables from the manifest
     *
     * is called whenever we get a new sstable that is repaired and we only have
     * unrepaired data.
     * @return all sstables in the manifest
     */
    public Collection<SSTableReader> drain()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (List<SSTableReader> level : generations)
        {
            sstables.addAll(level);
        }
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
        }
        hasRepairedData = null;
        return sstables;
    }

    public void setLastCompactedKey(int minLevel, DecoratedKey value)
    {
        lastCompactedKeys[minLevel] = value;
    }

    public SSTableReader findPotentiallyDroppableSSTable(final int gcBefore, float tombstoneThreshold)
    {
        level:
        for (int i = getLevelCount(); i >= 0; i--)
        {
            // sort sstables by droppable ratio in descending order
            SortedSet<SSTableReader> sstables = getLevelSorted(i, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    double r1 = o1.getEstimatedDroppableTombstoneRatio(gcBefore);
                    double r2 = o2.getEstimatedDroppableTombstoneRatio(gcBefore);
                    return -1 * Doubles.compare(r1, r2);
                }
            });
            if (sstables.isEmpty())
                continue;

            Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
            for (SSTableReader sstable : sstables)
            {
                if (sstable.getEstimatedDroppableTombstoneRatio(gcBefore) <= tombstoneThreshold)
                    continue level;
                else if (!compacting.contains(sstable) && !sstable.isMarkedSuspect())
                    return sstable;
            }
        }
        return null;
    }

    public static class CompactionCandidate implements Comparable<CompactionCandidate>
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }

        public int compareTo(CompactionCandidate o)
        {
            return Integer.compare(sstables.size(), o.sstables.size());

        }
    }
}
