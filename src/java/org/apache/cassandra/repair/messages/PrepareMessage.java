package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDSerializer;


public class PrepareMessage extends RepairMessage
{
    public final static MessageSerializer serializer = new PrepareMessageSerializer();
    public final String keyspace;
    public final Collection<Range<Token>> ranges;
    public final boolean fullRepair;
    public final String columnFamily;
    public final UUID parentRepairSession;

    public PrepareMessage(UUID parentRepairSession, String keyspace, String columnFamily, Collection<Range<Token>> ranges, boolean fullRepair)
    {
        super(Type.PREPARE_MESSAGE, null);
        this.parentRepairSession = parentRepairSession;
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.fullRepair = fullRepair;
        this.columnFamily = columnFamily;
    }

    public static class PrepareMessageSerializer implements MessageSerializer<PrepareMessage>
    {
        public void serialize(PrepareMessage message, DataOutput out, int version) throws IOException
        {
            out.writeUTF(message.keyspace);
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
            out.writeInt(message.ranges.size());
            for (Range r : message.ranges)
                Range.serializer.serialize(r, out, version);
            out.writeBoolean(message.fullRepair);
            out.writeUTF(message.columnFamily);

        }

        public PrepareMessage deserialize(DataInput in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.serializer.deserialize(in, version).toTokenBounds());
            boolean fullRepair = in.readBoolean();
            String columnFamily = in.readUTF();
            return new PrepareMessage(parentRepairSession, keyspace, columnFamily, ranges, fullRepair);
        }

        public long serializedSize(PrepareMessage message, int version)
        {
            long size;
            TypeSizes sizes = TypeSizes.NATIVE;
            size = sizes.sizeof(message.keyspace);
            size += UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
            size += sizes.sizeof(message.ranges.size());
            for (Range r : message.ranges)
            {
                size += Range.serializer.serializedSize(r, version);
            }
            size += sizes.sizeof(message.fullRepair);
            size += sizes.sizeof(message.columnFamily);
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "PrepareMessage{" +
                "keyspace='" + keyspace + '\'' +
                ", ranges=" + ranges +
                ", fullRepair=" + fullRepair +
                ", columnFamily='" + columnFamily + '\'' +
                ", parentRepairSession=" + parentRepairSession +
                '}';
    }
}
