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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.UUIDGen;

public class ManyToManyCompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(ManyToManyCompactionTask.class);

    private final int gcBefore;
    private CompactionManager.CompactionExecutorStatsCollector collector;
    private final AbstractCompactionStrategy.CompactionAwareWriter writer;

    /**
     * @param cfs
     */
    public ManyToManyCompactionTask(ColumnFamilyStore cfs, int gcBefore, AbstractCompactionStrategy.CompactionAwareWriter writer)
    {
        super(cfs, writer.getSSTables());
        this.gcBefore = gcBefore;
        this.writer = writer;
    }

    @Override
    protected int executeInternal(CompactionManager.CompactionExecutorStatsCollector collector)
    {
        this.collector = collector;
        run();
        return sstables.size();
    }

    /**
     * note that we ignore sstableDirectory, we write many sstables need to find the best spot for every one
     * @param sstableDirectory sstable directory to work on
     * @throws Exception
     */
    @Override
    protected void runWith(File sstableDirectory) throws Exception
    {
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();

        UUID taskId = SystemKeyspace.startCompaction(cfs, sstables);

        StringBuilder ssTableLoggerMsg = new StringBuilder("[");
        for (SSTableReader sstr : sstables)
            ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));

        ssTableLoggerMsg.append("]");
        String taskIdLoggerMsg = taskId == null ? UUIDGen.getTimeUUID().toString() : taskId.toString();
        logger.info("Compacting ({}) {}", taskIdLoggerMsg, ssTableLoggerMsg);
        long start = System.nanoTime();
        try (CompactionController controller = new CompactionController(cfs, sstables, gcBefore))
        {
            try (AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(sstables))
            {
                AbstractCompactionIterable ci = new CompactionIterable(compactionType, scanners.scanners, controller);
                try
                {
                    if (collector != null)
                        collector.beginCompaction(ci);

                    for (AbstractCompactedRow row : ci)
                    {
                        if (ci.isStopRequested())
                            throw new CompactionInterruptedException(ci.getCompactionInfo());

                        writer.append(row);
                    }
                    writer.close();
                }
                finally
                {
                    if (taskId != null)
                        SystemKeyspace.finishCompaction(taskId);
                    if (collector != null)
                        collector.finishCompaction(ci);


                }

                cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, writer.finished(), compactionType);
                long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                long startsize = SSTableReader.getTotalBytes(sstables);
                long endsize = SSTableReader.getTotalBytes(writer.finished());
                double ratio = (double) endsize / (double) startsize;
                String mergeSummary = CompactionTask.updateCompactionHistory(cfs.keyspace.getName(), cfs.getColumnFamilyName(), ci, startsize, endsize);
                StringBuilder newSSTableNames = new StringBuilder();
                for (SSTableReader reader : writer.finished())
                    newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                double mbps = dTime > 0 ? (double) endsize / (1024 * 1024) / ((double) dTime / 1000) : 0;
                logger.info(String.format("Compacted (%s) %d sstables to [%s]. %,d bytes to %,d (~%d%% of original) in %,dms = %fMB/s. Partition merge counts were {%s}",
                                          taskIdLoggerMsg, sstables.size(), newSSTableNames.toString(), startsize, endsize, (int) (ratio * 100), dTime, mbps, mergeSummary));

            }
        }
    }

    public SSTableWriter createWriter(File sstableDirectory, long keysPerSSTable, long repairedAt, int level)
    {
        return new SSTableWriter(cfs.getTempSSTablePath(sstableDirectory),
                                 keysPerSSTable,
                                 repairedAt,
                                 cfs.metadata,
                                 cfs.partitioner,
                                 new MetadataCollector(sstables, cfs.metadata.comparator, level, true));
    }

    @Override
    public long getExpectedWriteSize()
    {
        // we dont actually use the disk aware runnable here - it would write the entire compaction to the same drive
        return 0;
    }
}
