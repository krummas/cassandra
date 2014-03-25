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

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DiskAwareRunnable;

public abstract class AbstractCompactionTask extends DiskAwareRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCompactionTask.class);
    protected final ColumnFamilyStore cfs;
    protected Set<SSTableReader> sstables;
    protected boolean isUserDefined;
    protected OperationType compactionType;
    protected final File directory;
    /**
     * @param cfs
     * @param sstables must be marked compacting
     */
    public AbstractCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
    {
        this.cfs = cfs;
        this.sstables = sstables;
        this.isUserDefined = false;
        this.compactionType = OperationType.COMPACTION;

        // enforce contract that caller should mark sstables compacting
        Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
        Set<File> directories = new HashSet<>();
        if (!Iterables.isEmpty(sstables))
        {

            for (SSTableReader sstable : sstables)
            {
                directories.add(sstable.descriptor.directory);
                assert compacting.contains(sstable) : sstable.getFilename() + " is not correctly marked compacting";
            }
        }
        if (directories.size() == 1)
            directory = directories.iterator().next();
        else if (directories.size() == 0)
            directory = cfs.directories.getDirectoryForCompactedSSTables();
        else
        {
            logger.warn("Compaction contains sstables from different directories - you should run nodetool <TODO> to ensure the data is placed correctly on the disks. Placing compaction result on disk with most space.");
            File maxAvail = null;
            for (File dir : directories)
            {
                if (maxAvail == null || maxAvail.getFreeSpace() < dir.getFreeSpace())
                {
                    if (cfs.directories.getLocationForDisk(new Directories.DataDirectory(dir)) != null)
                        maxAvail = dir;
                }
            }
            if (maxAvail == null)
                directory = cfs.directories.getDirectoryForCompactedSSTables();
            else
                directory = maxAvail;
        }
    }

    protected Directories.DataDirectory getWriteableLocation()
    {
        return new Directories.DataDirectory(directory);
    }

    /**
     * executes the task and unmarks sstables compacting
     */
    public int execute(CompactionExecutorStatsCollector collector)
    {
        try
        {
            return executeInternal(collector);
        }
        finally
        {
            cfs.getDataTracker().unmarkCompacting(sstables);
        }
    }

    protected abstract int executeInternal(CompactionExecutorStatsCollector collector);

    protected Directories getDirectories()
    {
        return cfs.directories;
    }

    public AbstractCompactionTask setUserDefined(boolean isUserDefined)
    {
        this.isUserDefined = isUserDefined;
        return this;
    }

    public AbstractCompactionTask setCompactionType(OperationType compactionType)
    {
        this.compactionType = compactionType;
        return this;
    }

    public String toString()
    {
        return "CompactionTask(" + sstables + ")";
    }
}
