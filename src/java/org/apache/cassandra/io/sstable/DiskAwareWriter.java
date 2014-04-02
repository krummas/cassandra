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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;

public class DiskAwareWriter
{
    private final File[] compactionFileLocations;
    private final ColumnFamilyStore cfs;
    private final long expectedKeyCount;
    private final SSTableReader sstable;
    private final long repairedAt;
    private int boundaryIndex;
    private SSTableWriter writer;
    private final List<Token> diskBoundaries;
    private final List<SSTableWriter> writers = new ArrayList<>();
    private int totalKeysWritten;
    private int markKeysWritten;

    public DiskAwareWriter(ColumnFamilyStore cfs, File[] compactionFileLocations, long expectedKeyCount, long repairedAt, SSTableReader sstable)
    {
        boundaryIndex = 0;
        List<Range<Token>> localRanges = Range.normalize(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
        diskBoundaries = cfs.partitioner.splitRanges(localRanges, compactionFileLocations.length);
        this.compactionFileLocations = compactionFileLocations;
        this.cfs = cfs;
        this.expectedKeyCount = expectedKeyCount;
        this.sstable = sstable;
        this.repairedAt = repairedAt;
        writer = createWriter(cfs,
                              compactionFileLocations[boundaryIndex],
                              expectedKeyCount,
                              repairedAt,
                              sstable);
    }

    public RowIndexEntry append(AbstractCompactedRow row)
    {
        maybeSwitchWriter(row.key);
        RowIndexEntry rie = writer.append(row);
        if (rie != null)
            totalKeysWritten++;
        return rie;

    }

    private void maybeSwitchWriter(DecoratedKey key)
    {
        if (diskBoundaries == null) return;
        while (key.compareTo(diskBoundaries.get(boundaryIndex).maxKeyBound()) > 0)
        {
            boundaryIndex++;
            if (writer.getFilePointer() > 0)
                writers.add(writer);
            else
                writer.abort();
            writer = createWriter(cfs,
                                  compactionFileLocations[boundaryIndex],
                                  expectedKeyCount,
                                  repairedAt,
                                  sstable);
        }
    }
    public List<SSTableWriter> getWriters()
    {
        List<SSTableWriter> retWriters = new ArrayList<>(writers);
        if (writer.getFilePointer() > 0)
            retWriters.add(writer);
        return retWriters;
    }

    public List<SSTableReader> finish()
    {
        return finish(repairedAt);
    }

    public List<SSTableReader> finish(long overriddenRepairedAt)
    {
        List<SSTableReader> readers = new ArrayList<>();
        if (writer.getFilePointer() > 0)
            writers.add(writer);
        else
            writer.abort();

        for (SSTableWriter w : writers)
        {
            long maxDataAge = System.currentTimeMillis();
            if (sstable != null)
                maxDataAge = sstable.maxDataAge;
            readers.add(w.closeAndOpenReader(maxDataAge, overriddenRepairedAt));
        }
        return readers;
    }

    public void abort()
    {
        writer.abort();
        for(SSTableWriter writer : writers)
            writer.abort();
    }

    public int totalKeysWritten()
    {
        return totalKeysWritten;
    }

    public void append(DecoratedKey key, ColumnFamily cf)
    {
        maybeSwitchWriter(key);
        writer.append(key, cf);
        totalKeysWritten++;
    }

    public void mark()
    {
        writer.mark();
        markKeysWritten = totalKeysWritten;
    }

    public void resetAndTruncate()
    {
        writer.resetAndTruncate();
        totalKeysWritten = markKeysWritten;
    }

    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Descriptor.Version inputVersion) throws IOException
    {
        maybeSwitchWriter(key);
        totalKeysWritten++;
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
            return new SSTableWriter(cfs.getTempSSTablePath(compactionFileLocation),
                    expectedBloomFilterSize,
                    repairedAt,
                    cfs.metadata,
                    cfs.partitioner,
                    new MetadataCollector(Collections.singleton(sstable), cfs.metadata.comparator, sstable.getSSTableLevel()));
        }
        else
        {
            return createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, repairedAt);
        }
    }

    public int getTotalKeysWritten()
    {
        return totalKeysWritten;
    }

    public Descriptor currentDesc()
    {
        return writer.descriptor;
    }

    public long getCurrentSSTableSize()
    {
        return writer.getOnDiskFilePointer();
    }
}
