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


import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class LeveledManifestWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifestWrapper.class);
    private final ColumnFamilyStore cfs;
    private final int maxSSTableSizeInMB;
    private final SizeTieredCompactionStrategyOptions options;
    private List<Range<Token>> ranges = null;
    private List<LeveledManifest> manifests = null;
    private long rangesBasedOnRingVersion;
    @VisibleForTesting
    Set<SSTableReader> l0 = new HashSet<>();

    public LeveledManifestWrapper(ColumnFamilyStore cfs, int maxSSTableSizeInMB, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInMB = maxSSTableSizeInMB;
        this.options = options;
        initialize(cfs, maxSSTableSizeInMB);
    }


    /**
     * We need to do lazy initialization since we create the wrapper before we can figure out
     * the local ranges.
     * @param cfs
     * @param maxSSTableSizeInMB
     */
    private void initialize(ColumnFamilyStore cfs, int maxSSTableSizeInMB)
    {
        if (DatabaseDescriptor.getNumTokens() == 1)
        {
            this.ranges=null;
            this.manifests = new ArrayList<>(1);
            LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSizeInMB);

            boolean hasRepairedData = false;
            for (SSTableReader sstable : cfs.getSSTables())
            {
                if (sstable.getSSTableLevel() > 0 && sstable.isRepaired())
                    hasRepairedData = true;
            }

            for (SSTableReader sstable : cfs.getSSTables())
            {
                add(sstable, manifest, hasRepairedData);
            }
            manifests.add(manifest);
        }
        else
        {
            List<Range<Token>> newRanges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
            assert newRanges.size() > 0;

            if (this.ranges == null || !newRanges.equals(ranges))
            {
                this.ranges = newRanges;

                this.manifests = new ArrayList<>(ranges.size());

                List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTables());

                Collections.sort(sstables, new Comparator<SSTableReader>()
                {
                    @Override
                    public int compare(SSTableReader o1, SSTableReader o2)
                    {
                        return o1.originalFirst.compareTo(o2.originalFirst);
                    }
                });

                int sstableIndex = 0;
                for (int i = 1; i <= ranges.size(); i++)
                {
                    RowPosition rightBound = (i < ranges.size()) ? ranges.get(i).left.minKeyBound() : cfs.partitioner.getMaximumToken().maxKeyBound();
                    LeveledManifest m = new LeveledManifest(cfs, maxSSTableSizeInMB);
                    Set<SSTableReader> vnodeSSTables = new HashSet<>();
                    boolean hasRepairedData = false;
                    while (sstableIndex < sstables.size() && sstables.get(sstableIndex).originalFirst.compareTo(rightBound) < 0)
                    {
                        SSTableReader sstable = sstables.get(sstableIndex);
                        if (sstable.isRepaired())
                            hasRepairedData = true;
                        vnodeSSTables.add(sstable);
                        sstableIndex++;
                    }
                    for (SSTableReader sstable : vnodeSSTables)
                        add(sstable, m, hasRepairedData);
                    manifests.add(m);
                }
                assert sstableIndex == sstables.size() : "sstable index = " + sstableIndex + " sstables size = " + sstables.size();
                assert manifests.size() == ranges.size() : "manifests size = " + manifests.size() + " ranges size = " + ranges.size();
            }
        }
        rangesBasedOnRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
    }

    public synchronized void add(SSTableReader sstable)
    {
        if (sstable.getSSTableLevel() > 0)
        {
            LeveledManifest manifest = getManifestForSSTable(sstable);

            // we got a repaired sstable for a manifest with no repaired data;
            // we drain all sstables and add them to l0 without changing the
            // actual level since it is likely we can re-add them at the correct position
            // later:
            if ((sstable.isRepaired() && !manifest.hasRepairedData()))
                l0.addAll(manifest.drain());

            if (manifest.canAddSSTable(sstable))
                manifest.add(sstable);
            else
                sendToL0(sstable);
        }
        else
        {
            logger.info("adding {} in l0", sstable);
            if (!l0.add(sstable)) throw new AssertionError();
        }
    }
    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        logger.debug("Replacing {} with {}", removed, added);
        int minLevel = Integer.MAX_VALUE;
        Map<LeveledManifest, DecoratedKey> lastCompactedKeysPerManifest = new HashMap<>();
        for (SSTableReader sstable : removed)
        {
            minLevel = Math.min(minLevel, sstable.getSSTableLevel());
            if (sstable.getSSTableLevel() == 0)
            {
                boolean r = l0.remove(sstable);
                assert r : "Could not remove: " + sstable;
            }
            else
            {
                LeveledManifest manifest = getManifestForSSTable(sstable);
                if (manifest.hasRepairedData() && !sstable.isRepaired())
                {
                    boolean r = l0.remove(sstable);
                    assert r : "Could not remove "+sstable;
                    continue;
                }
                manifest.remove(sstable);

                DecoratedKey lastCompactedKey = lastCompactedKeysPerManifest.get(manifest);
                if (lastCompactedKey == null || lastCompactedKey.compareTo(sstable.last) < 0)
                    lastCompactedKeysPerManifest.put(manifest, sstable.last);
            }
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        for (SSTableReader sstable : added)
            add(sstable);

        for (Map.Entry<LeveledManifest, DecoratedKey> entry : lastCompactedKeysPerManifest.entrySet())
        {
            LeveledManifest manifest = entry.getKey();
            manifest.setLastCompactedKey(minLevel, entry.getValue());
        }
    }

    public synchronized void repairStatusChanged(Collection<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
        {
            if (sstable.getSSTableLevel() > 0)
            {
                LeveledManifest manifest = getManifestForSSTable(sstable);
                manifest.remove(sstable);
                add(sstable);
            }
        }
    }

    public synchronized boolean hasRepairedData(SSTableReader sstable)
    {
        return getManifestForSSTable(sstable).hasRepairedData();
    }

    public synchronized LeveledManifest.CompactionCandidate getCompactionCandidate()
    {
        List<SSTableReader> repairedCandidatesL0 = new ArrayList<>();
        List<SSTableReader> unrepairedCandidatesL0 = new ArrayList<>();
        for (SSTableReader sstable : cfs.getDataTracker().getUncompactingSSTables(l0))
        {
            if (!sstable.isMarkedSuspect())
            {
                if (sstable.isRepaired())
                    repairedCandidatesL0.add(sstable);
                else
                    unrepairedCandidatesL0.add(sstable);
            }
        }

        List<LeveledManifest.CompactionCandidate> candidatesL0 = getCandidatesForSTCSInL0(Arrays.asList(repairedCandidatesL0, unrepairedCandidatesL0));
        if (candidatesL0 != null)
            return Collections.max(candidatesL0);

        LeveledManifest.CompactionCandidate biggestCandidate = null;
        for (LeveledManifest manifest : manifests)
        {
            LeveledManifest.CompactionCandidate candidate = manifest.getCompactionCandidates();

            if (candidate != null && (biggestCandidate == null || candidate.sstables.size() > biggestCandidate.sstables.size()))
                biggestCandidate = candidate;
        }
        if (biggestCandidate != null)
            return biggestCandidate;

        boolean onlyRepairedManifests=true, onlyUnrepairedManifests=true;
        for (LeveledManifest m : manifests)
        {
            if (m.hasRepairedData()) onlyUnrepairedManifests = false;
            else onlyRepairedManifests = false;
        }

        // during a short period we can have mixed state in the manifests
        // since files in L0 most often intersect with all L1 files, we wait until all manifests are repaired.
        // todo: this can be improved, we can have state where a repaired L0 sstable only intersects with repaired l1 files.
        // todo: perhaps we should do a L0 STCS round here instead of returning null
        if (!onlyRepairedManifests && !onlyUnrepairedManifests)
            return null;

        List<SSTableReader> l0candidates = onlyRepairedManifests ? repairedCandidatesL0 : unrepairedCandidatesL0;
        if (l0candidates.size() > 0)
        {
            return getCandidateForL0toL1(l0candidates);
        }

        return null;
    }

    private LeveledManifest.CompactionCandidate getCandidateForL0toL1(List<SSTableReader> l0candidates)
    {

        // L0 is the dumping ground for new sstables which thus may overlap each other.
        //
        // We treat L0 compactions specially:
        // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
        // 1b. prefer choosing older sstables as candidates, to newer ones
        // 1c. any L0 sstables that overlap a candidate, will also become candidates
        // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
        // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
        //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
        //
        // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
        // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
        // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
        Set<SSTableReader> candidates = new HashSet<>();
        Set<SSTableReader> remaining = new HashSet<>(l0candidates);
        Set<SSTableReader> compactingL0 = Sets.intersection(Sets.newHashSet(l0), cfs.getDataTracker().getCompacting());

        for (SSTableReader sstable : ageSortedSSTables(remaining))
        {
            if (candidates.contains(sstable))
                continue;

            Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining));
            if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                continue;

            for (SSTableReader newCandidate : overlappedL0)
            {
                // overlappedL0 could contain sstables that are not in compactingL0, but do overlap
                // other sstables that are
                if (overlapping(newCandidate, compactingL0).isEmpty())
                    candidates.add(newCandidate);
                remaining.remove(newCandidate);
            }

            if (candidates.size() > LeveledManifest.MAX_COMPACTING_L0)
            {
                // limit to only the MAX_COMPACTING_L0 oldest candidates
                candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, LeveledManifest.MAX_COMPACTING_L0));
                break;
            }
        }

        // leave everything in L0 if we didn't end up with a full sstable's worth of data
        if (SSTableReader.getTotalBytes(candidates) > maxSSTableSizeInMB * (1 << 20))
        {
            // add sstables from L1 that overlap candidates
            // if the overlapping ones are already busy in a compaction, leave it out.
            // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
            for (LeveledManifest m : manifests)
            {
                candidates = Sets.union(candidates, overlapping(candidates, m.getLevel(1)));
            }
        }
        if (candidates.size() < 2)
            return null;
        else
            return new LeveledManifest.CompactionCandidate(candidates, 1, cfs.getCompactionStrategy().getMaxSSTableBytes());

    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
        return ageSortedCandidates;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.originalFirst.getToken();
        Token last = sstable.originalFirst.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.originalFirst.getToken()) <= 0 ? first : sstable.originalFirst.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }
    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<>();
        Bounds<Token> promotedBounds = new Bounds<>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<>(candidate.originalFirst.getToken(), candidate.last.getToken());
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        Set<SSTableReader> overlapped = new HashSet<>();
        for (SSTableReader candidate : others)
        {
            if (candidate.intersects(sstable))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    /**
     * Returns a list of candidates in L0
     *
     * The list typically contains a list of repaired and a list of unrepaired sstables
     *
     * we return a list of non-intersecting candidates (and, not mixing repaired/unrepaired)
     *
     * @param candidatesL0
     * @return
     */
    private List<LeveledManifest.CompactionCandidate> getCandidatesForSTCSInL0(List<List<SSTableReader>> candidatesL0)
    {
        List<LeveledManifest.CompactionCandidate> candidates = new ArrayList<>();
        for (List<SSTableReader> repUnrep : candidatesL0)
        {
            for (List<SSTableReader> disjointL0 : SizeTieredCompactionStrategy.splitBucket(repUnrep))
            {
                // if we are behind in L0, we do STCS there, these files stay in L0
                // todo: evaluate if this is a good limit now;
                if (disjointL0.size() > LeveledManifest.MAX_COMPACTING_L0)
                {
                    logger.info("Too many sstables in l0 ({}), running STCS", disjointL0.size());
                    List<List<SSTableReader>> mostInterestingSplit = SizeTieredCompactionStrategy.splitBucket(getSSTablesForSTCS(cfs, disjointL0, options));
                    for (List<SSTableReader> mostInteresting : mostInterestingSplit)
                    {
                        if (mostInteresting.size() > 1)
                            candidates.add(new LeveledManifest.CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE));
                    }
                }
            }
        }
        if (candidates.size() > 0)
            return candidates;
        return null;
    }

    public static List<SSTableReader> getSSTablesForSTCS(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        Iterable<SSTableReader> candidates = cfs.getDataTracker().getUncompactingSSTables(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);
        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, 4, 32);
    }

    public synchronized int getEstimatedTasks()
    {
        int tasks = 0;
        for (LeveledManifest manifest : manifests)
            tasks += manifest.getEstimatedTasks();
        long maxSSTableSizeInBytes = maxSSTableSizeInMB * (1<<20);
        tasks += Math.max(0L, SSTableReader.getTotalBytes(l0) - 4 * maxSSTableSizeInBytes) / maxSSTableSizeInBytes;

        return tasks;
    }

    private void add(SSTableReader sstable, LeveledManifest manifest, boolean hasRepairedData)
    {
        if (hasRepairedData && !sstable.isRepaired())
        {
            if(!l0.add(sstable)) throw new AssertionError();
        }
        else if (sstable.getSSTableLevel() > 0)
        {
            if (manifest.canAddSSTable(sstable))
                manifest.add(sstable);
            else
                sendToL0(sstable);
        }
        else
        {
            if(!l0.add(sstable)) throw new AssertionError();
        }
    }

    private void sendToL0(SSTableReader sstable)
    {
        logger.info("Could not add sstable {} (level={}) to the manifest, adding it to l0, ({} -> {})", sstable, sstable.getSSTableLevel(), sstable.originalFirst, sstable.last);
        try
        {
           sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
           sstable.reloadSSTableMetadata();
        }
        catch (IOException e)
        {
           logger.error("Could not mutate sstable level, adding it to l0");
        }
        logger.info("4 adding {}", sstable);
        if(!l0.add(sstable)) throw new AssertionError();
    }

    private LeveledManifest getManifestForSSTable(SSTableReader sstable)
    {
        if (sstable.getSSTableLevel() == 0) return null;

        if (StorageService.instance.getTokenMetadata().getRingVersion() != rangesBasedOnRingVersion)
        {
            logger.info("Reinitializing LeveledManifestWrapper due to ring changes");
            initialize(cfs, maxSSTableSizeInMB);
        }

        if (ranges == null)
            return manifests.get(0);
        int idx = Collections.binarySearch(ranges, new Range<>(sstable.originalFirst.getToken(), sstable.last.getToken()), new Comparator<Range<Token>>()
        {
            @Override
            public int compare(Range<Token> o1, Range<Token> o2)
            {
                if (o1.contains(o2.left))
                    return 0;
                return o1.left.minKeyBound().compareTo(o2.left.minKeyBound());
            }
        });

        // we want everything from the partitioner mintoken to left in the second range to go in the first (index 0) manifest:
        if (idx == -1)
            idx = 0;
        else if (idx < 0)
            idx = -idx - 2;
        return manifests.get(idx);
    }

    public synchronized List<LeveledManifest> manifests()
    {
        return manifests;
    }

    public synchronized int getLevelSize(int level)
    {
        if (level == 0)
            return l0.size();
        int size = 0;
        for (LeveledManifest manifest : manifests)
            size += manifest.getLevelSize(level);
        return size;
    }
    public synchronized int[] getAllLevelSize()
    {
        // TODO: fix!
        return null;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("L0=").append(l0.size()).append("\n");
        for (int i = 1; i < LeveledManifest.LEVEL_COUNT; i ++)
        {
            sb.append("L").append(i).append("=").append(getLevelSize(i)).append("\n");
        }
        return sb.toString();
    }

    public List<SSTableReader> findPotentiallyDroppableSSTables(int gcBefore, float tombstoneThreshold)
    {
        List<SSTableReader> potentiallyDroppable = new ArrayList<>();
        for (LeveledManifest m : manifests)
        {
            SSTableReader sstable = m.findPotentiallyDroppableSSTable(gcBefore, tombstoneThreshold);
            if (sstable != null)
                potentiallyDroppable.add(sstable);
        }
        return potentiallyDroppable;
    }
}
