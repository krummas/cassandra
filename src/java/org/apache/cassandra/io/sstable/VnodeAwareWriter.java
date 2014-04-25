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
package org.apache.cassandra.io.sstable;


import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;

public class VnodeAwareWriter
{
    private final File[] compactionFileLocations;
    private final ColumnFamilyStore cfs;
    private final long expectedKeyCount;
    private final SSTableReader sstable;
    private final long repairedAt;
    private final List<SSTableWriter> writers;
    private int boundaryIndex;
    private int vnodeIndex;
    private final SSTableRewriter writer;
    private List<Token> diskBoundaries;
    private final List<Token> vnodeBoundaries;

    public VnodeAwareWriter(ColumnFamilyStore cfs, File[] compactionFileLocations, long expectedKeyCount, long repairedAt, OperationType rewriteType, SSTableReader sstable)
    {
        boundaryIndex = 0;
        vnodeIndex = 0;
        this.cfs = cfs;
        vnodeBoundaries = createVnodeBoundaries(Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName())));
        diskBoundaries = cfs.partitioner.splitRanges(compactionFileLocations.length);
        if (diskBoundaries == null || vnodeBoundaries.size() == 0)
        {
            diskBoundaries = Arrays.asList(cfs.partitioner.getMaximumToken());
            this.compactionFileLocations = new File[] { cfs.directories.getDirectoryForCompactedSSTables() };
        }
        else
        {
            this.compactionFileLocations = compactionFileLocations;
        }
        this.expectedKeyCount = expectedKeyCount;
        this.sstable = sstable;
        this.repairedAt = repairedAt;
        this.writers = new ArrayList<>();
        writer = new SSTableRewriter(cfs, new HashSet<>(Collections.singleton(sstable)), sstable.maxDataAge, rewriteType, false);
        SSTableWriter sstableWriter = createWriter(cfs,
                                                 compactionFileLocations[boundaryIndex],
                                                 expectedKeyCount,
                                                 repairedAt,
                                                 sstable);
        writer.switchWriter(sstableWriter);
        writers.add(sstableWriter);
    }

    /**
     * everything from partitioner minToken up to first token of the second vnode goes in the first sstable
     *
     * everything from first token of the last sstable to partitioner maxToken goes in last sstable.
     * @param localRanges
     * @return
     */
    private List<Token> createVnodeBoundaries(List<Range<Token>> localRanges)
    {
        List<Token> results = new ArrayList<>();
        for (int i = 1; i < localRanges.size() - 1; i++)
        {
            Range<Token> r = localRanges.get(i);
            results.add(r.left);

        }
        results.add(cfs.partitioner.getMaximumToken());
        return results;
    }

    public RowIndexEntry append(AbstractCompactedRow row)
    {
        maybeSwitchWriter(row.key);
        return writer.append(row);

    }

    private void maybeSwitchWriter(DecoratedKey key)
    {
        if (diskBoundaries == null) return;

        // find the vnode containing the key:
        while (key.compareTo(vnodeBoundaries.get(vnodeIndex).minKeyBound()) > 0)
        {
            vnodeIndex++;
            // find the disk containing the vnode:
            while (vnodeBoundaries.get(vnodeIndex).compareTo(diskBoundaries.get(boundaryIndex)) > 0)
            {
                boundaryIndex++;
            }
            SSTableWriter sstableWriter = createWriter(cfs,
                                                       compactionFileLocations[boundaryIndex],
                                                       expectedKeyCount,
                                                       repairedAt,
                                                       sstable);
            writer.switchWriter(sstableWriter);
            writers.add(sstableWriter);
        }
    }

    public List<SSTableWriter> getWriters()
    {
        return writers;
    }

    public List<SSTableReader> finish()
    {
        return finish(repairedAt);
    }

    public List<SSTableReader> finish(long overriddenRepairedAt)
    {
        writer.finish(overriddenRepairedAt);
        return writer.finished();
    }

    public void abort()
    {
        writer.abort();
    }

    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Descriptor.Version inputVersion) throws IOException
    {
        maybeSwitchWriter(key);
        return writer.appendFromStream(key, metadata, in, inputVersion);
    }


    private static SSTableWriter createWriter(ColumnFamilyStore cfs, File location, long expectedKeyCount, long repairedAt)
    {
        Descriptor desc = Descriptor.fromFilename(cfs.getTempSSTablePath(location));

        return new SSTableWriter(desc.filenameFor(Component.DATA), expectedKeyCount, repairedAt);
    }

    private static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             long expectedBloomFilterSize,
                                             long repairedAt,
                                             SSTableReader sstable)
    {
        FileUtils.createDirectory(compactionFileLocation);
        if (sstable != null)
        {
            int sstableLevel = 0;
            if (sstable.descriptor.directory.equals(compactionFileLocation))
                sstableLevel = sstable.getSSTableLevel();
            return new SSTableWriter(cfs.getTempSSTablePath(compactionFileLocation),
                                     expectedBloomFilterSize,
                                     repairedAt,
                                     cfs.metadata,
                                     cfs.partitioner,
                                     new MetadataCollector(Collections.singleton(sstable), cfs.metadata.comparator, sstableLevel));
        }
        else
        {
            return createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, repairedAt);
        }
    }

    public Descriptor currentDesc()
    {
        return writer.currentWriter().descriptor;
    }
}
