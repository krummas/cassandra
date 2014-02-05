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
    public final List<UUID> cfIds;
    public final Collection<Range<Token>> ranges;
    public final boolean fullRepair;

    public final UUID parentRepairSession;

    public PrepareMessage(UUID parentRepairSession, List<UUID> cfIds, Collection<Range<Token>> ranges, boolean fullRepair)
    {
        super(Type.PREPARE_MESSAGE, null);
        this.parentRepairSession = parentRepairSession;
        this.cfIds = cfIds;
        this.ranges = ranges;
        this.fullRepair = fullRepair;
    }

    public static class PrepareMessageSerializer implements MessageSerializer<PrepareMessage>
    {
        public void serialize(PrepareMessage message, DataOutput out, int version) throws IOException
        {
            out.writeInt(message.cfIds.size());
            for (UUID cfId : message.cfIds)
                UUIDSerializer.serializer.serialize(cfId, out, version);
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
            out.writeInt(message.ranges.size());
            for (Range r : message.ranges)
                Range.serializer.serialize(r, out, version);
            out.writeBoolean(message.fullRepair);
        }

        public PrepareMessage deserialize(DataInput in, int version) throws IOException
        {
            int cfIdCount = in.readInt();
            List<UUID> cfIds = new ArrayList<>(cfIdCount);
            for (int i = 0; i < cfIdCount; i++)
                cfIds.add(UUIDSerializer.serializer.deserialize(in, version));
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.serializer.deserialize(in, version).toTokenBounds());
            boolean fullRepair = in.readBoolean();
            return new PrepareMessage(parentRepairSession, cfIds, ranges, fullRepair);
        }

        public long serializedSize(PrepareMessage message, int version)
        {
            long size;
            TypeSizes sizes = TypeSizes.NATIVE;
            size = sizes.sizeof(message.cfIds.size());
            for (UUID cfId : message.cfIds)
                size += UUIDSerializer.serializer.serializedSize(cfId, version);
            size += UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
            size += sizes.sizeof(message.ranges.size());
            for (Range r : message.ranges)
            {
                size += Range.serializer.serializedSize(r, version);
            }
            size += sizes.sizeof(message.fullRepair);
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "PrepareMessage{" +
                "cfIds='" + cfIds + '\'' +
                ", ranges=" + ranges +
                ", fullRepair=" + fullRepair +
                ", parentRepairSession=" + parentRepairSession +
                '}';
    }
}
