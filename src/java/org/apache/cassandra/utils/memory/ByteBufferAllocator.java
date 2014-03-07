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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

public interface ByteBufferAllocator
{
    ByteBuffer clone(ByteBuffer buffer);
    ByteBuffer allocate(int size);

    public static abstract class AbstractAllocator implements ByteBufferAllocator
    {
        public ByteBuffer clone(ByteBuffer buffer)
        {
            assert buffer != null;
            if (buffer.remaining() == 0)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBuffer cloned = allocate(buffer.remaining());

            cloned.mark();
            cloned.put(buffer.duplicate());
            cloned.reset();
            return cloned;
        }
    }
}
