/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapPool extends ByteBufferPool
{
    public HeapPool(long maxOnHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, 0, cleanupThreshold, cleaner);
    }

    public Allocator newAllocator()
    {
        return new Allocator(onHeap.newAllocator(), offHeap.newAllocator());
    }
    public boolean needToCopyOnHeap()
    {
        return false;
    }

    public static final class Allocator extends ByteBufferPool.Allocator
    {
        Allocator(SubAllocator onHeap, SubAllocator offHeap)
        {
            super(onHeap, offHeap);
        }

        public ByteBuffer allocate(int size)
        {
            return allocate(size, null);
        }

        public ByteBuffer allocate(int size, OpOrder.Group opGroup)
        {
            onHeap.allocate(size, opGroup);
            // must loop trying to acquire
            return ByteBuffer.allocate(size);
        }

        public void free(ByteBuffer name)
        {
            onHeap.release(name.remaining());
        }

    }

}
