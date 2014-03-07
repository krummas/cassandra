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
package org.apache.cassandra.db.data;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public class BufferDeletedCell extends BufferCell implements DeletedCell
{
    public BufferDeletedCell(CellName name, int localDeletionTime, long timestamp)
    {
        this(name, ByteBufferUtil.bytes(localDeletionTime), timestamp);
    }

    public BufferDeletedCell(CellName name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferDeletedCell(newName, value(), timestamp());
    }
    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferDeletedCell(name(), value(), newTimestamp);
    }

    @Override
    public boolean isMarkedForDelete(long now)
    {
        return DeletedCell.Impl.isMarkedForDelete(now);
    }
    @Override
    public long getMarkedForDeleteAt()
    {
        return DeletedCell.Impl.getMarkedForDeleteAt(this);
    }
    @Override
    public void updateDigest(MessageDigest digest)
    {
        DeletedCell.Impl.updateDigest(this, digest);
    }
    @Override
    public int getLocalDeletionTime()
    {
        return DeletedCell.Impl.getLocalDeletionTime(this);
    }
    @Override
    public Cell reconcile(Cell cell)
    {
        return DeletedCell.Impl.reconcile(this, cell);
    }
    @Override
    public DeletedCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return DeletedCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    @Override
    public DeletedCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    @Override
    public int serializationFlags()
    {
        return DeletedCell.Impl.serializationFlags();
    }
    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        DeletedCell.Impl.validateFields(this, metadata);
    }
    @Override
    public boolean equals(Object that)
    {
        return DeletedCell.Impl.equals(this, that);
    }
}
