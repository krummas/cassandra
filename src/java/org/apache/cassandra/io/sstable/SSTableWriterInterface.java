package org.apache.cassandra.io.sstable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.Pair;

public interface SSTableWriterInterface
{
    void mark();
    void resetAndTruncate();
    RowIndexEntry append(AbstractCompactedRow row);
    void append(DecoratedKey decoratedKey, ColumnFamily cf);
    long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Descriptor.Version version) throws IOException;
    void abort();
    Collection<SSTableReader> closeAndOpenReader();
    Collection<SSTableReader> closeAndOpenReader(long maxDataAge);
    Collection<SSTableReader> closeAndOpenReader(long maxDataAge, long repairedAt);
    Pair<Descriptor, StatsMetadata> close();
    boolean hasDataWritten();
    long dataWritten();
    CFMetaData getMetadata();
    Descriptor getDescriptor();
}
