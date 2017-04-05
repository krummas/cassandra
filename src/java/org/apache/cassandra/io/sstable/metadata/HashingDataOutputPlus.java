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

package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

import com.google.common.base.Function;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Hashes the input
 *
 * NOTE: all hasher.put* methods need to map directly against the methods in HashingDataInputPlus - for example
 * the writeShort(int) / short readShort() - here we must use putInt in both methods since writeShort gets an integer as input!
 */
public class HashingDataOutputPlus implements DataOutputPlus
{
    private final DataOutputPlus out;
    private final Hasher hasher = Hashing.md5().newHasher();

    public HashingDataOutputPlus(DataOutputPlus out)
    {
        this.out = out;
    }
    public void write(ByteBuffer buffer) throws IOException
    {
        // need to clone because buffer might not have an array
        ByteBuffer toHash = ByteBufferUtil.clone(buffer);
        out.write(buffer);
        hasher.putBytes(toHash.array(), toHash.arrayOffset() + toHash.position(), toHash.remaining());
    }

    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    public <R> R applyToChannel(Function<WritableByteChannel, R> c) throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void write(int b) throws IOException
    {
        out.write(b);
        hasher.putInt(b);
    }

    public void write(byte[] b) throws IOException
    {
        out.write(b);
        hasher.putBytes(b);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        out.write(b);
        hasher.putBytes(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException
    {
        out.writeBoolean(v);
        hasher.putBoolean(v);
    }

    public void writeByte(int v) throws IOException
    {
        out.writeByte(v);
        hasher.putInt(v);
    }

    public void writeShort(int v) throws IOException
    {
        out.writeShort(v);
        hasher.putInt(v);
    }

    public void writeChar(int v) throws IOException
    {
        out.writeChar(v);
        hasher.putInt(v);
    }

    public void writeInt(int v) throws IOException
    {
        out.writeInt(v);
        hasher.putInt(v);
    }

    public void writeLong(long v) throws IOException
    {
        out.writeLong(v);
        hasher.putLong(v);
    }

    public void writeFloat(float v) throws IOException
    {
        out.writeFloat(v);
        hasher.putFloat(v);
    }

    public void writeDouble(double v) throws IOException
    {
        out.writeDouble(v);
        hasher.putDouble(v);
    }

    public void writeBytes(String s) throws IOException
    {
        out.writeBytes(s);
        hasher.putUnencodedChars(s);
    }

    public void writeChars(String s) throws IOException
    {
        out.writeChars(s);
        hasher.putUnencodedChars(s);
    }

    public void writeUTF(String s) throws IOException
    {
        out.writeUTF(s);
        hasher.putString(s, Charset.forName("UTF-8"));
    }

    public long getHash()
    {
        return hasher.hash().asLong();
    }

    public void writeVInt(long i) throws IOException
    {
        out.writeVInt(i);
        hasher.putLong(i);
    }

    public void writeUnsignedVInt(long i) throws IOException
    {
        out.writeUnsignedVInt(i);
        hasher.putLong(i);
    }

    public long position()
    {
        return out.position();
    }

    public boolean hasPosition()
    {
        return out.hasPosition();
    }
}
