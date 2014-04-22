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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Functions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.utils.CLibrary;

public class SSTableRewriter
{

    private static final long preemptiveOpenInterval;
    static
    {
        long interval = DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB() * (1L << 20);
        if (interval < 0)
            interval = Long.MAX_VALUE;
        preemptiveOpenInterval = interval;
    }

    private final DataTracker dataTracker;
    private final ColumnFamilyStore cfs;

    private final long maxAge;
    private final Set<SSTableReader> rewriting;
    private final Map<Descriptor, DecoratedKey> originalStarts = new HashMap<>();
    private final Map<Descriptor, Integer> fileDescriptors = new HashMap<>();

    private SSTableReader currentlyOpenedEarly;
    private long currentlyOpenedEarlyAt;

    private final List<SSTableReader> finished = new ArrayList<>();
    private final OperationType rewriteType;
    private final boolean isOffline;

    private SSTableWriter writer;
    private Map<DecoratedKey, RowIndexEntry> cachedKeys = new HashMap<>();

    public SSTableRewriter(ColumnFamilyStore cfs, Set<SSTableReader> rewriting, long maxAge, OperationType rewriteType, boolean isOffline)
    {
        this.rewriting = new HashSet<>(rewriting);
        for (SSTableReader sstable : rewriting)
        {
            originalStarts.put(sstable.descriptor, sstable.first);
            fileDescriptors.put(sstable.descriptor, CLibrary.getfd(sstable.getFilename()));
        }
        this.dataTracker = cfs.getDataTracker();
        this.cfs = cfs;
        this.maxAge = maxAge;
        this.rewriteType = rewriteType;
        this.isOffline = isOffline;
    }

    public SSTableWriter currentWriter()
    {
        return writer;
    }

    public void mark()
    {
        writer.mark();
    }

    public RowIndexEntry append(AbstractCompactedRow row)
    {
        maybeReopenEarly(row.key);
        RowIndexEntry index = writer.append(row);
        if (!isOffline)
        {
            if (index == null)
            {
                cfs.invalidateCachedRow(row.key);
            }
            else
            {
                boolean save = false;
                for (SSTableReader reader : rewriting)
                {
                    if (reader.getCachedPosition(row.key, false) != null)
                    {
                        save = true;
                        break;
                    }
                }
                if (save)
                    cachedKeys.put(row.key, index);
            }
        }
        return index;
    }

    private void maybeReopenEarly(DecoratedKey key)
    {
        if (writer.getFilePointer() - currentlyOpenedEarlyAt > preemptiveOpenInterval)
        {
            if (isOffline)
            {
                for (SSTableReader reader : rewriting)
                {
                    RowIndexEntry index = reader.getPosition(key, SSTableReader.Operator.GE);
                    CLibrary.trySkipCache(fileDescriptors.get(reader.descriptor), 0, index == null ? 0 : index.position);
                }
            }
            else
            {
                SSTableReader reader = writer.openEarly(maxAge);
                if (reader != null)
                {
                    for (Map.Entry<DecoratedKey, RowIndexEntry> cacheKey : cachedKeys.entrySet())
                        reader.cacheKey(cacheKey.getKey(), cacheKey.getValue());
                    replaceReader(currentlyOpenedEarly, reader);
                    currentlyOpenedEarly = reader;
                    currentlyOpenedEarlyAt = writer.getFilePointer();
                    moveStarts(reader, Functions.constant(reader.last), false);
                }
            }
        }
    }

    public void resetAndTruncate()
    {
        writer.resetAndTruncate();
    }

    public void abort()
    {
        if (writer == null)
            return;
        moveStarts(null, Functions.forMap(originalStarts), true);
        List<SSTableReader> close = new ArrayList<>(finished);
        if (currentlyOpenedEarly != null)
            close.add(currentlyOpenedEarly);
        // also remove already completed SSTables
        for (SSTableReader sstable : close)
            sstable.markObsolete();
        // releases reference in replaceReaders
        dataTracker.replaceReaders(close, Collections.<SSTableReader>emptyList());
        dataTracker.unmarkCompacting(close);
        writer.abort();
    }

    private void moveStarts(SSTableReader newReader, Function<? super Descriptor, DecoratedKey> newStarts, boolean reset)
    {
        if (isOffline)
            return;
        List<SSTableReader> toReplace = new ArrayList<>();
        List<SSTableReader> replaceWith = new ArrayList<>();
        final List<DecoratedKey> invalidateKeys = new ArrayList<>();
        if (!reset)
        {
            invalidateKeys.addAll(cachedKeys.keySet());
            if (newReader != null)
            {
                for (Map.Entry<DecoratedKey, RowIndexEntry> cacheKey : cachedKeys.entrySet())
                    newReader.cacheKey(cacheKey.getKey(), cacheKey.getValue());
            }
        }
        cachedKeys = new HashMap<>();
        for (final SSTableReader sstable : rewriting)
        {
            DecoratedKey newStart = newStarts.apply(sstable.descriptor);
            assert newStart != null;
            if (sstable.first.compareTo(newStart) < 0 || (reset && newStart != sstable.first))
            {
                toReplace.add(sstable);
                // we call getCurrentReplacement() to support multiple rewriters operating over the same source readers at once.
                // note: only one such writer should be written to at any moment
                replaceWith.add(sstable.getCurrentReplacement().cloneWithNewStart(newStart, new Runnable()
                {
                    public void run()
                    {
                        // this is somewhat racey, in that we could theoretically be closing this old reader
                        // when an even older reader is still in use, but it's not likely to have any major impact
                        for (DecoratedKey key : invalidateKeys)
                            sstable.invalidateCacheKey(key);
                    }
                }));
            }
        }
        replaceReaders(toReplace, replaceWith);
        rewriting.removeAll(toReplace);
        rewriting.addAll(replaceWith);
    }

    private void replaceReader(SSTableReader toReplace, SSTableReader replaceWith)
    {
        if (toReplace != null)
            toReplace.setReplacedBy(replaceWith);
        replaceReaders(Collections.singletonList(toReplace).subList(0, toReplace == null ? 0 : 1), Collections.singletonList(replaceWith));
    }

    private void replaceReaders(Collection<SSTableReader> toReplace, Collection<SSTableReader> replaceWith)
    {
        if (isOffline)
            return;
        dataTracker.replaceReaders(toReplace, replaceWith);
    }

    public void switchWriter(SSTableWriter newWriter)
    {
        if (writer == null)
        {
            writer = newWriter;
            return;
        }
        // tmp = false because later we want to query it with descriptor from SSTableReader
        SSTableReader reader = writer.closeAndOpenReader(maxAge);
        finished.add(reader);
        replaceReader(currentlyOpenedEarly, reader);
        moveStarts(reader, Functions.constant(reader.last), false);
        currentlyOpenedEarly = null;
        currentlyOpenedEarlyAt = 0;
        writer = newWriter;
    }

    public void finish()
    {
        finish(-1);
    }
    public void finish(long repairedAt)
    {
        finish(true, repairedAt);
    }
    public void finish(boolean cleanupOldReaders, long repairedAt)
    {
        if (writer.getFilePointer() > 0)
        {
            SSTableReader reader = repairedAt < 0 ?
                                    writer.closeAndOpenReader(maxAge) :
                                    writer.closeAndOpenReader(maxAge, repairedAt);
            finished.add(reader);
            replaceReader(currentlyOpenedEarly, reader);
            moveStarts(reader, Functions.constant(reader.last), false);
        }
        else
        {
            writer.abort();
            writer = null;
        }

        if (!isOffline)
        {
            dataTracker.unmarkCompacting(finished);
            if (cleanupOldReaders)
                dataTracker.markCompactedSSTablesReplaced(rewriting, finished, rewriteType);
        }
        else if (cleanupOldReaders)
        {
            for (SSTableReader reader : rewriting)
            {
                reader.markObsolete();
                reader.releaseReference();
            }
        }
    }

    public List<SSTableReader> finished()
    {
        return finished;
    }
}
