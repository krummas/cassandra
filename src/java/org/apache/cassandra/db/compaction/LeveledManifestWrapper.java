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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class LeveledManifestWrapper
{
    private final ColumnFamilyStore cfs;
    private final int maxSSTableSize;
    private final SizeTieredCompactionStrategyOptions options;
    private List<Range<Token>> ranges = null;
    private List<LeveledManifest> manifests = null;
    private boolean initialized = false;

    public LeveledManifestWrapper(ColumnFamilyStore cfs, int maxSSTableSize, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.maxSSTableSize = maxSSTableSize;
        this.options = options;
    }

    public synchronized void add(SSTableReader reader)
    {
        LeveledManifest manifest = getManifestForSSTable(reader);
        manifest.add(reader);
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);

        LeveledManifest manifest = null;
        for (SSTableReader r : removed)
        {
            LeveledManifest m = getManifestForSSTable(r);
            assert manifest == null || m == manifest;
            manifest = m;
        }
        for (SSTableReader r : added)
        {
            LeveledManifest m = getManifestForSSTable(r);
            assert manifest == null || m == manifest;
            manifest = m;
        }
        if (manifest != null)
            manifest.replace(removed, added);
    }

    public synchronized void repairStatusChanged(Collection<SSTableReader> sstables)
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);
        List<List<SSTableReader>> sstablesPerVnode = groupSSTablesPerVnode(sstables, ranges);
        for (List<SSTableReader> vnodeSSTables : sstablesPerVnode)
        {
            if (vnodeSSTables.size() > 0)
            {
                LeveledManifest manifest = getManifestForSSTable(vnodeSSTables.get(0));
                manifest.repairStatusChanged(vnodeSSTables);
            }
        }
    }

    public boolean hasRepairedData(SSTableReader sstable)
    {
        return getManifestForSSTable(sstable).hasRepairedData();
    }

    public synchronized LeveledManifest.CompactionCandidate getCompactionCandidates()
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);
        LeveledManifest.CompactionCandidate biggestCandidate = null;
        for (LeveledManifest manifest : manifests)
        {
            LeveledManifest.CompactionCandidate candidate = manifest.getCompactionCandidates();

            if (candidate != null && (biggestCandidate == null || candidate.sstables.size() > biggestCandidate.sstables.size()))
                biggestCandidate = candidate;
        }
        return biggestCandidate;
    }

    public int getEstimatedTasks()
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);
        int tasks = 0;
        for (LeveledManifest manifest : manifests)
            tasks += manifest.getEstimatedTasks();
        return tasks;
    }


    private static List<List<SSTableReader>> groupSSTablesPerVnode(Collection<SSTableReader> origSSTables, List<Range<Token>> ranges)
    {
        List<SSTableReader> sstables = new ArrayList<>(origSSTables);

        if (ranges == null) return Arrays.asList(sstables);

        Collections.sort(sstables, SSTableReader.sstableComparator);
        List<List<SSTableReader>> result = new ArrayList<>();
        List<SSTableReader> oneVnode = new ArrayList<>();
        int rangeIndex = 1;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.first.compareTo(ranges.get(rangeIndex).left.minKeyBound()) < 0)
                oneVnode.add(sstable);
            else
            {
                result.add(oneVnode);
                oneVnode = new ArrayList<>();

                while (sstable.first.compareTo(ranges.get(rangeIndex).left.minKeyBound()) > 0)
                    rangeIndex++;
                oneVnode.add(sstable);
            }
        }
        return result;
    }

    private void lazyInitialize(ColumnFamilyStore cfs, int maxSSTableSize,  SizeTieredCompactionStrategyOptions options)
    {
        if (DatabaseDescriptor.getNumTokens() == 1)
        {
            this.ranges=null;
            this.manifests = new ArrayList<>(1);
            manifests.add(LeveledManifest.create(cfs, maxSSTableSize, cfs.getSSTables(), options));
        }
        else
        {
            this.ranges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
            if (ranges.size() == 0) return;
            this.manifests = new ArrayList<>(ranges.size());

            for (Range<Token> r : ranges)
            {
                List<SSTableReader> sstablesForVnode = cfs.getSSTablesForVnode(r);
                this.manifests.add(LeveledManifest.create(cfs, maxSSTableSize, sstablesForVnode, options));
            }
        }
        initialized = true;
    }

    private LeveledManifest getManifestForSSTable(SSTableReader reader)
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);

        if (ranges == null)
            return manifests.get(0);
        int idx = Collections.binarySearch(ranges, new Range<>(reader.first.token, reader.last.token), new Comparator<Range<Token>>()
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
        if (idx == -1) idx = 0;
        else if (idx < 0) idx = -idx - 2;

        return manifests.get(idx);
    }

    public List<LeveledManifest> manifests()
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);

        return manifests;
    }

    public int getLevelSize(int level)
    {
        if (!initialized)
            lazyInitialize(cfs, maxSSTableSize, options);
        int size = 0;
        for (LeveledManifest manifest : manifests)
            size += manifest.getLevelSize(level);
        return size;
    }
    public int[] getAllLevelSize()
    {
        // TODO: fix!
        return null;
    }
}
