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
import java.nio.charset.Charset;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.io.util.DataInputPlus;

/**
 * Hashes the input
 *
 * NOTE: all hasher.put* methods need to map directly against the methods in HashingDataOutputPlus - for example
 * the writeShort(int) / short readShort() - here we must use putInt in both methods since writeShort gets an integer as input!
 */
public class HashingDataInputPlus implements DataInputPlus
{
    private final DataInputPlus in;
    private final Hasher hasher = Hashing.md5().newHasher();

    public HashingDataInputPlus(DataInputPlus in)
    {
        this.in = in;
    }

    public void readFully(byte[] b) throws IOException
    {
        in.readFully(b);
        hasher.putBytes(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        in.readFully(b, off, len);
        hasher.putBytes(b, off, len);
    }

    public int skipBytes(int n) throws IOException
    {
        return in.skipBytes(n);
    }

    public boolean readBoolean() throws IOException
    {
        boolean b = in.readBoolean();
        hasher.putBoolean(b);
        return b;
    }

    public byte readByte() throws IOException
    {
        byte b = in.readByte();
        hasher.putInt(b);
        return b;
    }

    public int readUnsignedByte() throws IOException
    {
        int b = in.readUnsignedByte();
        hasher.putInt(b);
        return b;
    }

    public short readShort() throws IOException
    {
        short s = in.readShort();
        hasher.putInt(s);
        return s;
    }

    public int readUnsignedShort() throws IOException
    {
        int s = in.readUnsignedShort();
        hasher.putInt(s);
        return s;
    }

    public char readChar() throws IOException
    {
        char c = in.readChar();
        hasher.putInt(c);
        return c;
    }

    public int readInt() throws IOException
    {
        int i = in.readInt();
        hasher.putInt(i);
        return i;
    }

    public long readLong() throws IOException
    {
        long l = in.readLong();
        hasher.putLong(l);
        return l;
    }

    public float readFloat() throws IOException
    {
        float f = in.readFloat();
        hasher.putFloat(f);
        return f;
    }

    public double readDouble() throws IOException
    {
        double d = in.readDouble();
        hasher.putDouble(d);
        return d;
    }

    public String readLine() throws IOException
    {
        String l = in.readLine();
        hasher.putUnencodedChars(l);
        return l;
    }

    public String readUTF() throws IOException
    {
        String l = in.readUTF();
        hasher.putString(l, Charset.forName("UTF-8"));
        return l;
    }

    public long readVInt() throws IOException
    {
        long vint = in.readVInt();
        hasher.putLong(vint);
        return vint;
    }

    public long readUnsignedVInt() throws IOException
    {
        long vint = in.readUnsignedVInt();
        hasher.putLong(vint);
        return vint;
    }
    public void skipBytesFully(int n) throws IOException
    {
        in.skipBytesFully(n);
    }

    public long getHash()
    {
        return hasher.hash().asLong();
    }
}
