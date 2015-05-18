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

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * The default compaction writer - creates one output file in L0
 */
public class DefaultCompactionWriter extends CompactionAwareWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(DefaultCompactionWriter.class);
    private final SSTableRewriter sstableWriter;
    private final Set<SSTableReader> allSSTables;

    public DefaultCompactionWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, boolean offline, OperationType compactionType)
    {
        super(cfs, nonExpiredSSTables, nonExpiredSSTables, offline);
        this.allSSTables = allSSTables;
        logger.debug("Expected bloom filter size : {}", estimatedTotalKeys);
        sstableWriter = new SSTableRewriter(cfs, allSSTables, maxAge, offline);
    }

    @Override
    protected boolean realAppend(AbstractCompactedRow row)
    {
        return sstableWriter.append(row) != null;
    }

    @Override
    protected boolean realTryAppend(AbstractCompactedRow row)
    {
        return sstableWriter.tryAppend(row) != null;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        sstableWriter.switchWriter(SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(location))),
                                                            estimatedTotalKeys,
                                                            minRepairedAt,
                                                            cfs.metadata,
                                                            cfs.partitioner,
                                                            new MetadataCollector(allSSTables, cfs.metadata.comparator, 0)));

    }

    @Override
    public List<SSTableReader> finish(long repairedAt)
    {
        return sstableWriter.setRepairedAt(repairedAt).finish();
    }

    @Override
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }
}