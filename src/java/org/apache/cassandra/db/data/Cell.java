package org.apache.cassandra.db.data;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

/**
 * Cells should be immutable, which prevents all kinds of confusion in a multithreaded environment.
 */
public interface Cell extends OnDiskAtom
{
    public static final int MAX_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    Cell withUpdatedName(CellName newName);

    Cell withUpdatedTimestamp(long newTimestamp);

    CellName name();

    ByteBuffer value();

    long timestamp();

    long minTimestamp();

    long maxTimestamp();

    boolean isMarkedForDelete(long now);

    boolean isLive(long now);

    // Don't call unless the column is actually marked for delete.
    long getMarkedForDeleteAt();

    int cellDataSize();

    // returns the size of the Cell and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    long unsharedHeapSizeExcludingData();

    int serializedSize(CellNameType type, TypeSizes typeSizes);

    int serializationFlags();

    Cell diff(Cell cell);

    void updateDigest(MessageDigest digest);

    int getLocalDeletionTime();

    Cell reconcile(Cell cell);

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    Cell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator);

    Cell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp);

    String getString(CellNameType comparator);

    void validateFields(CFMetaData metadata) throws MarshalException;

    public static class Impl
    {
        /**
         * For 2.0-formatted sstables (where column count is not stored), @param count should be Integer.MAX_VALUE,
         * and we will look for the end-of-row column name marker instead of relying on that.
         */
        public static Iterator<OnDiskAtom> onDiskIterator(final DataInput in,
                                                          final int count,
                                                          final ColumnSerializer.Flag flag,
                                                          final int expireBefore,
                                                          final Descriptor.Version version,
                                                          final CellNameType type)
        {
            return new AbstractIterator<OnDiskAtom>()
            {
                int i = 0;

                protected OnDiskAtom computeNext()
                {
                    if (i++ >= count)
                        return endOfData();

                    OnDiskAtom atom;
                    try
                    {
                        atom = type.onDiskAtomSerializer().deserializeFromSSTable(in, flag, expireBefore, version);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                    if (atom == null)
                        return endOfData();

                    return atom;
                }
            };
        }

        static boolean isMarkedForDelete(Cell me, long now)
        {
            return false;
        }

        static boolean isLive(Cell me, long now)
        {
            return !me.isMarkedForDelete(now);
        }

        // Don't call unless the column is actually marked for delete.
        static long getMarkedForDeleteAt()
        {
            return Long.MAX_VALUE;
        }

        static int cellDataSize(Cell me)
        {
            return me.name().dataSize() + me.value().remaining() + TypeSizes.NATIVE.sizeof(me.timestamp());
        }

        static int serializedSize(Cell me, CellNameType type, TypeSizes typeSizes)
        {
        /*
         * Size of a column is =
         *   size of a name (short + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */
            int valueSize = me.value().remaining();
            return ((int)type.cellSerializer().serializedSize(me.name(), typeSizes)) + 1 + typeSizes.sizeof(me.timestamp()) + typeSizes.sizeof(valueSize) + valueSize;
        }

        static int serializationFlags()
        {
            return 0;
        }

        static Cell diff(Cell me, Cell other)
        {
            if (me.timestamp() < other.timestamp())
                return other;
            return null;
        }

        static void updateDigest(Cell me, MessageDigest digest)
        {
            digest.update(me.name().toByteBuffer().duplicate());
            digest.update(me.value().duplicate());

            DataOutputBuffer buffer = new DataOutputBuffer(9);
            try
            {
                buffer.writeLong(me.timestamp());
                buffer.writeByte(me.serializationFlags());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            digest.update(buffer.getData(), 0, buffer.getLength());
        }

        static int getLocalDeletionTime()
        {
            return Integer.MAX_VALUE;
        }

        static Cell reconcile(Cell me, Cell upd)
        {
            // tombstones take precedence.  (if both are tombstones, then it doesn't matter which one we use.)
            if (isMarkedForDelete(me, System.currentTimeMillis()))
                return me.timestamp() < upd.timestamp() ? upd : me;
            if (upd.isMarkedForDelete(System.currentTimeMillis()))
                return me.timestamp() > upd.timestamp() ? me : upd;
            // break ties by comparing values.
            if (me.timestamp() == upd.timestamp())
                return me.value().compareTo(upd.value()) < 0 ? upd : me;
            // neither is tombstoned and timestamps are different
            return me.timestamp() < upd.timestamp() ? upd : me;
        }

        static Cell localCopy(Cell me, CFMetaData cfMetaData, ByteBufferAllocator allocator)
        {
            return new BufferCell(me.name().copy(cfMetaData, allocator), allocator.clone(me.value()), me.timestamp());
        }

        static String getString(Cell me, CellNameType comparator)
        {
            return String.format("%s:%b:%d@%d",
                                 comparator.getString(me.name()),
                                 isMarkedForDelete(me, System.currentTimeMillis()),
                                 me.value().remaining(),
                                 me.timestamp());
        }

        static void validateName(Cell me, CFMetaData metadata) throws MarshalException
        {
            metadata.comparator.validate(me.name());
        }

        static void validateFields(Cell me, CFMetaData metadata) throws MarshalException
        {
            validateName(me, metadata);

            AbstractType<?> valueValidator = metadata.getValueValidator(me.name());
            if (valueValidator != null)
                valueValidator.validate(me.value());
        }

        static boolean equals(Cell me, Object that)
        {
            if (that == me)
                return true;
            if (!(that instanceof Cell))
                return false;
            return equals(me, (Cell) that);
        }
        static boolean equals(Cell me, Cell that)
        {
            return that.name().equals(me.name()) && that.value().equals(me.value()) && that.timestamp() == me.timestamp();
        }
    }
}
