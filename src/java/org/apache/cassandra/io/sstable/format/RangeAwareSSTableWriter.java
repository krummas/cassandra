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
package org.apache.cassandra.io.sstable.format;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.service.StorageService;

public class RangeAwareSSTableWriter
{

    private final List<RowPosition> boundaries;
    private final Directories.DataDirectory[] directories;
    private final int sstableLevel;
    private final long estimatedKeys;
    private final long repairedAt;
    private int currentIndex = 0;
    public final ColumnFamilyStore cfs;
    private final List<SSTableWriter> finishedWriters = new ArrayList<>();
    private SSTableWriter currentWriter = null;

    public RangeAwareSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, int sstableLevel, SSTableFormat.Type format, long totalSize) throws IOException
    {
        directories = cfs.directories.getWriteableLocations();
        this.sstableLevel = sstableLevel;
        this.cfs = cfs;
        this.estimatedKeys = estimatedKeys / directories.length;
        this.repairedAt = repairedAt;
        List<Range<Token>> localRanges;
        if (Schema.instance.getKeyspaceInstance(cfs.keyspace.getName()) != null)
            localRanges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
        else
            localRanges = Collections.emptyList();

        if (!cfs.partitioner.supportsSplitting() || localRanges.isEmpty())
        {
            Directories.DataDirectory localDir = cfs.directories.getWriteableLocation(totalSize);
            Descriptor desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir), format));
            currentWriter = SSTableWriter.create(desc, estimatedKeys, repairedAt, sstableLevel);
            boundaries = null;
        }
        else
        {
            if (DatabaseDescriptor.getNumTokens() > 1)
                boundaries = createBoundariesForVNodes(cfs, localRanges, directories);
            else
                boundaries = createBoundaries(cfs, localRanges, directories);
        }
    }


    private static List<RowPosition> createBoundaries(ColumnFamilyStore cfs, List<Range<Token>> localRanges, Directories.DataDirectory [] locations)
    {
        List<Token> boundaries = cfs.partitioner.splitRange(localRanges.get(0).left, localRanges.get(localRanges.size() - 1).right, locations.length);
        List<RowPosition> diskBoundaries = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++)
        {
            Token t = boundaries.get(i);
            diskBoundaries.add(t.minKeyBound());
        }
        diskBoundaries.add(cfs.partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }

    private static List<RowPosition> createBoundariesForVNodes(ColumnFamilyStore cfs, List<Range<Token>> localRanges,  Directories.DataDirectory [] locations)
    {
        List<Token> boundaries = cfs.partitioner.splitFullRange(locations.length);
        int boundaryIndex = 0;
        List<RowPosition> diskBoundaries = new ArrayList<>();
        for (Range<Token> localRange : localRanges)
        {
            while (localRange.left.minKeyBound().compareTo(boundaries.get(boundaryIndex).maxKeyBound()) > 0)
            {
                diskBoundaries.add(localRange.left.minKeyBound());
                boundaryIndex++;
            }
        }
        diskBoundaries.add(cfs.partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }


    private void verifyWriter(DecoratedKey key)
    {
        if (boundaries == null)
            return;
        boolean switched = false;
        while (key.compareTo(boundaries.get(currentIndex)) > 0)
        {
            switched = true;
            currentIndex++;
        }

        if (switched)
        {
            if (currentWriter != null)
                finishedWriters.add(currentWriter);
            Directories.DataDirectory dir = directories[currentIndex];
            String file = cfs.getTempSSTablePath( cfs.directories.getLocationForDisk(dir));
            currentWriter = SSTableWriter.create(file, estimatedKeys, repairedAt, sstableLevel);
        }
    }

    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Version version) throws IOException
    {
        verifyWriter(key);
        return currentWriter.appendFromStream(key, metadata, in, version);
    }

    public List<SSTableReader> close()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);

        List<SSTableReader> sstableReaders = new ArrayList<>();
        for (SSTableWriter writer : finishedWriters)
        {
            if (writer.getOnDiskFilePointer() > 0)
                sstableReaders.add(writer.closeAndOpenReader());
            else
                writer.abort();
        }
        return sstableReaders;
    }

    public void abort()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);

        for (SSTableWriter writer : finishedWriters)
            writer.abort();
    }

    public String getFilename()
    {
        if (currentWriter != null)
            return currentWriter.getFilename();
        return "null";
    }

    public List<SSTableWriter> getWriters()
    {
        return finishedWriters;
    }

    public SSTableWriter currentWriter()
    {
        return currentWriter;
    }
}
