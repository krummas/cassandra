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
package org.apache.cassandra.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriterInterface;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    // number of files to receive
    private final int totalFiles;
    // total size of files to receive
    private final long totalSize;
    private volatile boolean aborted;

    //  holds references to SSTables received
    protected Collection<SSTableWriterInterface> sstableWriters;

    public StreamReceiveTask(StreamSession session, UUID cfId, int totalFiles, long totalSize)
    {
        super(session, cfId);
        this.totalFiles = totalFiles;
        this.totalSize = totalSize;
        this.sstableWriters = new ArrayList<>(totalFiles);
    }

    /**
     * Process received file.
     *
     * @param writer SSTable file received.
     */
    public void received(SSTableWriterInterface writer)
    {
        assert cfId.equals(writer.getMetadata().cfId);
        assert !aborted;

        sstableWriters.add(writer);
        if (sstableWriters.size() == totalFiles)
            complete();
    }

    public int getTotalNumberOfFiles()
    {
        return totalFiles;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    private void complete()
    {
        if (!sstableWriters.isEmpty())
            StorageService.tasks.submit(new OnCompletionRunnable(this));
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }

        public void run()
        {
            Pair<String, String> kscf = Schema.instance.getCF(task.cfId);
            ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);
            // todo: should not use [0]
            StreamLockfile lockfile = new StreamLockfile(cfs.directories.getCompactionLocationsAsFiles()[0], UUID.randomUUID());
            lockfile.create(task.sstableWriters);
            List<SSTableReader> readers = new ArrayList<>();
            for (SSTableWriterInterface writer : task.sstableWriters)
                readers.addAll(writer.closeAndOpenReader());
            lockfile.delete();

            if (!SSTableReader.acquireReferences(readers))
                throw new AssertionError("We shouldn't fail acquiring a reference on a sstable that has just been transferred");
            try
            {
                // add sstables and build secondary indexes
                cfs.addSSTables(readers);
                cfs.indexManager.maybeBuildSecondaryIndexes(readers, cfs.indexManager.allIndexesNames());
            }
            finally
            {
                SSTableReader.releaseReferences(readers);
            }

            task.session.taskCompleted(task);
        }
    }

    public void abort()
    {
        aborted = true;
        Runnable r = new Runnable()
        {
            public void run()
            {
                for (SSTableWriterInterface writer : sstableWriters)
                    writer.abort();
            }
        };
        StorageService.tasks.submit(r);
    }
}
