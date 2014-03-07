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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;
import org.apache.cassandra.utils.memory.ByteBufferPool;

public class SimpleSparseCellName extends AbstractComposite implements CellName
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleSparseCellName(null));

    private final ColumnIdentifier columnName;

    // Not meant to be used directly, you should use the CellNameType method instead
    SimpleSparseCellName(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
    }

    public int size()
    {
        return 1;
    }

    public ByteBuffer get(int i)
    {
        if (i != 0)
            throw new IndexOutOfBoundsException();

        return columnName.bytes;
    }

    @Override
    public Composite withEOC(EOC newEoc)
    {
        // EOC makes no sense for not truly composites.
        return this;
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return columnName.bytes;
    }

    @Override
    public int dataSize()
    {
        return columnName.bytes.remaining();
    }

    public int clusteringSize()
    {
        return 0;
    }

    public ColumnIdentifier cql3ColumnName(CFMetaData metadata)
    {
        return columnName;
    }

    public ByteBuffer collectionElement()
    {
        return null;
    }

    public boolean isCollectionCell()
    {
        return false;
    }

    public boolean isSameCQL3RowAs(CellNameType type, CellName other)
    {
        return true;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + columnName.unsharedHeapSizeExcludingData();
    }

    public boolean equals(CellName that)
    {
        return super.equals(that);
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + columnName.unsharedHeapSize();
    }

    public CellName copy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return new SimpleSparseCellName(columnName.clone(allocator));
    }

    public void free(ByteBufferPool.Allocator allocator)
    {
        allocator.free(columnName.bytes);
    }
}
