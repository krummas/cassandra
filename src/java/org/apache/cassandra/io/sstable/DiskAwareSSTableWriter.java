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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class DiskAwareSSTableWriter implements SSTableWriterInterface
{
    private SSTableWriterInterface writer;
    private final String [] locations;
    private Token currentLimit;
    private final List<Token> boundaries;
    private final List<SSTableWriterInterface> finishedWriters;
    private int currentIndex = 0;
    private final long keyCount;
    private final long repairedAt;
    private final CFMetaData metadata;
    private final IPartitioner partitioner;
    private final int sstableLevel;
    private final Collection<SSTableReader> sstables;
    private Set<Integer> ancestors = new HashSet<>();
    private ReplayPosition context = null;

    public DiskAwareSSTableWriter(String [] fileLocations, int sstableLevel, long keyCount, long repairedAt, ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        finishedWriters = new ArrayList<>(fileLocations.length);
        this.repairedAt = repairedAt;
        this.keyCount = keyCount;
        this.metadata = cfs.metadata;
        this.partitioner = cfs.partitioner;
        this.sstableLevel = sstableLevel;
        locations = fileLocations;
        Collection<Range<Token>> localRanges = Range.normalize(StorageService.instance.getLocalRanges(cfs.metadata.ksName));
        boundaries = partitioner.splitRanges(localRanges, locations.length);
        this.sstables = sstables;
        if (sstables != null)
        {
            for (SSTableReader sstable : sstables)
            {
                ancestors.add(sstable.descriptor.generation);
                for (Integer i : sstable.getAncestors())
                {
                    if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                        ancestors.add(i);
                }
            }
        }
        updateWriter();
    }

    public DiskAwareSSTableWriter(String [] fileLocations, int sstableLevel, long keyCount, long repairedAt, ColumnFamilyStore cfs, ReplayPosition context)
    {
        this(fileLocations, sstableLevel, keyCount, repairedAt, cfs, (Collection<SSTableReader>)null);
        this.context = context;
    }

    public DiskAwareSSTableWriter(String [] fileLocations, int sstableLevel, long keyCount, long repairedAt, ColumnFamilyStore cfs)
    {
        this(fileLocations, sstableLevel, keyCount, repairedAt, cfs, (Collection<SSTableReader>)null);
    }

    private void updateWriter()
    {
        currentLimit = boundaries.get(currentIndex);
        MetadataCollector collector;
        if (context == null && sstables == null)
            collector = new MetadataCollector(metadata.comparator);
        else if (context == null)
            collector = new MetadataCollector(sstables, metadata.comparator, sstableLevel);
        else
        {
            collector = new MetadataCollector(metadata.comparator).replayPosition(context);
            collector.setAncestors(ancestors);
        }
        writer = new SSTableWriter(locations[currentIndex], keyCount, repairedAt, metadata, partitioner, collector);
        currentIndex++;
    }


    @Override
    public void mark()
    {
        writer.mark();
    }

    @Override
    public void resetAndTruncate()
    {
        writer.resetAndTruncate();
    }

    @Override
    public RowIndexEntry append(AbstractCompactedRow row)
    {
        maybeUpdateWriter(row.key.token);
        return writer.append(row);
    }

    @Override
    public void append(DecoratedKey decoratedKey, ColumnFamily cf)
    {
        maybeUpdateWriter(decoratedKey.token);
        writer.append(decoratedKey, cf);
    }

    @Override
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Descriptor.Version version) throws IOException
    {
        maybeUpdateWriter(key.token);
        return writer.appendFromStream(key, metadata, in, version);
    }

    private void maybeUpdateWriter(Token curToken)
    {
        while (curToken.compareTo(currentLimit) > 0)
        {
            if (writer.hasDataWritten())
                finishedWriters.add(writer);
            updateWriter();
        }
    }

    @Override
    public void abort()
    {
        finishedWriters.add(writer);
        for (SSTableWriterInterface writer : finishedWriters)
        {
            writer.abort();
        }
    }

    @Override
    public Collection<SSTableReader> closeAndOpenReader()
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    @Override
    public Collection<SSTableReader> closeAndOpenReader(long maxDataAge)
    {
        return closeAndOpenReader(maxDataAge, this.repairedAt);
    }

    @Override
    public Collection<SSTableReader> closeAndOpenReader(long maxDataAge, long repairedAt)
    {
        if (writer.hasDataWritten())
            finishedWriters.add(writer);
        List<SSTableReader> readers = new ArrayList<>(finishedWriters.size());
        for (SSTableWriterInterface writer : finishedWriters)
        {
            readers.addAll(writer.closeAndOpenReader(maxDataAge, repairedAt));
        }
        return readers;
    }

    @Override
    public Pair<Descriptor, StatsMetadata> close()
    {
        // TODO: handle this
        if (writer.hasDataWritten())
            finishedWriters.add(writer);
        for (SSTableWriterInterface writer : finishedWriters)
        {
            writer.close();
        }
        return writer.close();
    }

    @Override
    public long dataWritten()
    {
        long written = 0;
        for (SSTableWriterInterface writer : finishedWriters)
        {
            written += writer.dataWritten();
        }
        return written + writer.dataWritten();
    }

    @Override
    public boolean hasDataWritten()
    {
        return finishedWriters.size() > 0 || writer.hasDataWritten();
    }

    @Override
    public CFMetaData getMetadata()
    {
        return writer.getMetadata();
    }

    @Override
    public Descriptor getDescriptor()
    {
        return writer.getDescriptor();
    }

    @Override
    public String toString()
    {
        StringBuilder files = new StringBuilder();
        for (String s : locations)
        {
            files.append(s).append(",");
        }
        return files.toString();
    }
}
