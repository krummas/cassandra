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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;

public class HashingDataPlusTest
{
    private void testHelper(SilentFunction<HashingDataOutputPlus> output, SilentFunction<HashingDataInputPlus> input) throws Exception
    {
        DataOutputBuffer dob = new DataOutputBuffer(2000);
        HashingDataOutputPlus hdop = new HashingDataOutputPlus(dob);
        output.apply(hdop);
        long hash1 = hdop.getHash();
        ByteBuffer buffer = dob.buffer();
        buffer.rewind();
        DataInputBuffer dib = new DataInputBuffer(buffer, true);
        HashingDataInputPlus hdip = new HashingDataInputPlus(dib);
        input.apply(hdip);
        assertEquals(hdip.getHash(), hash1);
    }

    @Test
    public void testShort() throws Exception
    {
        testHelper((output) -> output.writeShort(123), HashingDataInputPlus::readShort);
        testHelper((output) -> output.writeShort(-123), HashingDataInputPlus::readShort);
        testHelper((output) -> output.writeShort(0), HashingDataInputPlus::readShort);
    }
    @Test
    public void testUnsignedShort() throws Exception
    {
        testHelper((output) -> output.writeShort(123), HashingDataInputPlus::readUnsignedShort);
        testHelper((output) -> output.writeShort(99), HashingDataInputPlus::readUnsignedShort);
        testHelper((output) -> output.writeShort(0), HashingDataInputPlus::readUnsignedShort);
    }
    @Test
    public void testBoolean() throws Exception
    {
        testHelper((output) -> output.writeBoolean(true), HashingDataInputPlus::readBoolean);
        testHelper((output) -> output.writeBoolean(false), HashingDataInputPlus::readBoolean);
    }

    @Test
    public void testByte() throws Exception
    {
        testHelper((output) -> output.writeByte(11), HashingDataInputPlus::readByte);
        testHelper((output) -> output.writeByte(-11), HashingDataInputPlus::readByte);
    }
    @Test
    public void testUnsignedByte() throws Exception
    {
        testHelper((output) -> output.writeByte(11), HashingDataInputPlus::readUnsignedByte);
    }

    @Test
    public void testChar() throws Exception
    {
        testHelper((output) -> output.writeChar('a'), HashingDataInputPlus::readChar);
    }

    @Test
    public void testInt() throws Exception
    {
        testHelper((output) -> output.writeInt(33), HashingDataInputPlus::readInt);
        testHelper((output) -> output.writeInt(Integer.MAX_VALUE), HashingDataInputPlus::readInt);
        testHelper((output) -> output.writeInt(Integer.MIN_VALUE), HashingDataInputPlus::readInt);
        testHelper((output) -> output.writeInt(0), HashingDataInputPlus::readInt);
    }

    @Test
    public void testLong() throws Exception
    {
        testHelper((output) -> output.writeLong(33), HashingDataInputPlus::readLong);
        testHelper((output) -> output.writeLong(Long.MAX_VALUE), HashingDataInputPlus::readLong);
        testHelper((output) -> output.writeLong(Long.MIN_VALUE), HashingDataInputPlus::readLong);
        testHelper((output) -> output.writeLong(0), HashingDataInputPlus::readLong);
    }

    @Test
    public void testFloat() throws Exception
    {
        testHelper((output) -> output.writeFloat(33), HashingDataInputPlus::readFloat);
        testHelper((output) -> output.writeFloat(Float.MAX_VALUE), HashingDataInputPlus::readFloat);
        testHelper((output) -> output.writeFloat(Float.MIN_VALUE), HashingDataInputPlus::readFloat);
        testHelper((output) -> output.writeFloat(0.2f), HashingDataInputPlus::readFloat);
    }

    @Test
    public void testDouble() throws Exception
    {
        testHelper((output) -> output.writeDouble(33), HashingDataInputPlus::readDouble);
        testHelper((output) -> output.writeDouble(Double.MAX_VALUE), HashingDataInputPlus::readDouble);
        testHelper((output) -> output.writeDouble(Double.MIN_VALUE), HashingDataInputPlus::readDouble);
        testHelper((output) -> output.writeDouble(0.2f), HashingDataInputPlus::readDouble);
    }

    @Test
    public void testStrings() throws Exception
    {
        testHelper((output) -> output.writeUTF("åäö"), HashingDataInputPlus::readUTF);
    }

    @Test
    public void testVints() throws Exception
    {
        testHelper((output) -> output.writeVInt(1234), HashingDataInputPlus::readVInt);
        testHelper((output) -> output.writeUnsignedVInt(1234), HashingDataInputPlus::readUnsignedVInt);
    }

    private interface SilentFunction<T>
    {
        void apply(T x) throws Exception;
    }

}
