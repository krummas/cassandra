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

package org.apache.cassandra.db.compaction.writers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.PartitionPosition.Kind.MAX_BOUND;
import static org.apache.cassandra.db.PartitionPosition.Kind.MIN_BOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiLevelWriterTest
{
    @Test
    public void subtractTest()
    {
        Collection<Bounds<PartitionPosition>> diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(10, 25));
        assertEquals(1, diff.size());
        Bounds<PartitionPosition> b = diff.iterator().next();
        testBounds(b, t(0).minKeyBound(), t(10).minKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(0, 10));
        assertEquals(1, diff.size());
        b = diff.iterator().next();
        testBounds(b, t(10).maxKeyBound(), t(20).maxKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(1, 19));
        assertEquals(2, diff.size());
        Iterator<Bounds<PartitionPosition>> it = diff.iterator();
        testBounds(it.next(), t(0).minKeyBound(), t(1).minKeyBound());
        testBounds(it.next(), t(19).maxKeyBound(), t(20).maxKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(21, 22));
        assertEquals(1, diff.size());
        testBounds(diff.iterator().next(), t(0).minKeyBound(), t(20).maxKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(-1, 21));
        assertEquals(0, diff.size());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), sstableBounds(20, 20));
        assertEquals(1, diff.size());
        testBounds(diff.iterator().next(), t(0).minKeyBound(), t(20).minKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), bounds(0, false, 20, false));
        assertEquals(2, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(0).minKeyBound(), t(0).maxKeyBound());
        testBounds(it.next(), t(20).minKeyBound(), t(20).maxKeyBound());

        diff = MultiLevelWriter.subtract(sstableBounds(0, 20), bounds(0, true, 20, false));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(20).minKeyBound(), t(20).maxKeyBound());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, true),
                                         bounds(20, true, 25, false));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(0).minKeyBound(), t(20).minKeyBound());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, false),
                                         bounds(20, true, 25, false));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(0).minKeyBound(), t(20).minKeyBound());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, false),
                                         bounds(0, true, 0, true));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(0).maxKeyBound(), t(20).minKeyBound());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, false),
                                         bounds(20, true, 40, true));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(0).minKeyBound(), t(20).minKeyBound());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, false),
                                         bounds(-1, true, 20, true));
        assertEquals(0, diff.size());
        diff = MultiLevelWriter.subtract(bounds(0, true, 20, false),
                                         bounds(-1, true, 21, true));
        assertEquals(0, diff.size());

        diff = MultiLevelWriter.subtract(bounds(0, true, 20, true),
                                         bounds(-1, true, 20, false));
        assertEquals(1, diff.size());
        it = diff.iterator();
        testBounds(it.next(), t(20).minKeyBound(), t(20).maxKeyBound());
    }

    private void testBounds(Bounds<PartitionPosition> b, Token.KeyBound left, Token.KeyBound right)
    {
        assertEquals(left.getToken().getTokenValue(), b.left.getToken().getTokenValue());
        assertEquals(left.kind(), b.left.kind());
        assertEquals(right.getToken().getTokenValue(), b.right.getToken().getTokenValue());
        assertEquals(right.kind(), b.right.kind());
    }

    @Test
    public void levelsTest()
    {
        MultiLevelWriter.Levels levels = new MultiLevelWriter.Levels(sstableBounds(0, 100));
        levels.addToken(0, t(0));
        levels.addToken(0, t(100));
        levels.addToken(1, t(20));
        levels.addToken(1, t(90));
        levels.addToken(2, t(30));
        levels.addToken(2, t(70));

        List<Pair<Long, Integer>> expected = new ArrayList<>();
        expected.add(Pair.create(0L, 0));
        expected.add(Pair.create(19L, 0));
        expected.add(Pair.create(20L, 1));
        expected.add(Pair.create(21L, 1));
        expected.add(Pair.create(29L, 1));
        expected.add(Pair.create(30L, 2));
        expected.add(Pair.create(31L, 2));
        expected.add(Pair.create(69L, 2));
        expected.add(Pair.create(70L, 2));
        expected.add(Pair.create(71L, 1));
        expected.add(Pair.create(90L, 1));
        expected.add(Pair.create(91L, 0));
        expected.add(Pair.create(100L, 0));

        for (Pair<Long, Integer> expect : expected)
        {
            long token = expect.left;
            int level = expect.right;
            assertTrue(contains(token, levels.getBoundariesForLevel(level)));
        }
    }

    @Test
    public void calculateRangeLevelsTest()
    {
        DatabaseDescriptor.daemonInitialization();
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader [] sst = new SSTableReader[10];
        sst[0] = MockSchema.sstable(1, 1, false, dk(0), dk(100), 0, cfs);

        sst[1] = MockSchema.sstable(2, 1, false, dk(10), dk(10), 1, cfs);
        sst[2] = MockSchema.sstable(3, 1, false, dk(11), dk(20), 1, cfs);
        sst[3] = MockSchema.sstable(4, 1, false, dk(30), dk(95), 1, cfs);

        sst[4] = MockSchema.sstable(5, 1, false, dk(4), dk(4), 2, cfs);
        sst[5] = MockSchema.sstable(6, 1, false, dk(5), dk(7), 2, cfs);
        sst[6] = MockSchema.sstable(7, 1, false, dk(10), dk(14), 2, cfs);
        sst[7] = MockSchema.sstable(8, 1, false, dk(20), dk(22), 2, cfs);
        sst[8] = MockSchema.sstable(9, 1, false, dk(42), dk(45), 2, cfs);
        sst[9] = MockSchema.sstable(10, 1, false, dk(77), dk(88), 2, cfs);
        List<Pair<Bounds<PartitionPosition>, Integer>> boundaries = new MultiLevelWriter.RangeLevels(Sets.newHashSet(sst), new Range<>(t(4), t(78))).rangeLevels;
        assertEquals(4, boundaries.size());

        assertEquals(0, boundaries.get(0).right.intValue());
        assertEquals(bounds(0, true, 4, false), boundaries.get(0).left);
        assertEquals(2, boundaries.get(1).right.intValue());
        assertEquals(bounds(4, true, 88, true), boundaries.get(1).left);
        assertEquals(1, boundaries.get(2).right.intValue());
        assertEquals(bounds(88, false, 95, true), boundaries.get(2).left);
        assertEquals(0, boundaries.get(3).right.intValue());
        assertEquals(bounds(95, false, 100, true), boundaries.get(3).left);
    }

    @Test
    public void calculateRangeLevelsTest2()
    {
        DatabaseDescriptor.daemonInitialization();
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader [] sst = new SSTableReader[3];
        sst[0] = MockSchema.sstable(1, 1, false, dk(0), dk(100), 0, cfs);
        sst[1] = MockSchema.sstable(2, 1, false, dk(50), dk(60), 1, cfs);
        sst[2] = MockSchema.sstable(3, 1, false, dk(30), dk(90), 2, cfs);
        List<Pair<Bounds<PartitionPosition>, Integer>> boundaries = new MultiLevelWriter.RangeLevels(Sets.newHashSet(sst), new Range<>(t(30), t(90))).rangeLevels;
        // level 0 is 0 -> 30 and 90 -> 100 below
        assertEquals(bounds(0, true, 30, false), boundaries.get(0).left);
        assertEquals(0, boundaries.get(0).right.intValue());
        assertEquals(bounds(30, true, 90, true), boundaries.get(1).left);
        assertEquals(2, boundaries.get(1).right.intValue());
        assertEquals(bounds(90, false, 100, true), boundaries.get(2).left);
        assertEquals(0, boundaries.get(2).right.intValue());
    }

    @Test
    public void calculateRangeLevelsEmptyLevel()
    {
        DatabaseDescriptor.daemonInitialization();
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader [] sst = new SSTableReader[3];
        sst[0] = MockSchema.sstable(1, 1, false, dk(0), dk(100), 0, cfs);
        sst[1] = MockSchema.sstable(2, 1, false, dk(20), dk(90), 2, cfs);
        sst[2] = MockSchema.sstable(3, 1, false, dk(30), dk(90), 3, cfs);
        List<Pair<Bounds<PartitionPosition>, Integer>> boundaries = new MultiLevelWriter.RangeLevels(Sets.newHashSet(sst), new Range<>(t(30), t(90))).rangeLevels;
        assertEquals(bounds(0, true, 20, false), boundaries.get(0).left);
        assertEquals(0, boundaries.get(0).right.intValue());
        assertEquals(bounds(20, true, 30, false), boundaries.get(1).left);
        assertEquals(2, boundaries.get(1).right.intValue());
        assertEquals(bounds(30, true, 90, true), boundaries.get(2).left);
        assertEquals(3, boundaries.get(2).right.intValue());
        assertEquals(bounds(90, false, 100, true), boundaries.get(3).left);
        assertEquals(0, boundaries.get(3).right.intValue());

    }

    private static Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static DecoratedKey dk(long t)
    {
        return new BufferDecoratedKey(t(t), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static boolean contains(long key, Bounds<PartitionPosition> p)
    {

        long leftBound = (long) p.left.getToken().getTokenValue();
        long rightBound = (long) p.right.getToken().getTokenValue();
        return leftBound < key && rightBound > key ||
               leftBound == key && p.left.kind() == MIN_BOUND ||
               rightBound == key && p.right.kind() == MAX_BOUND;
    }

    private static Bounds<PartitionPosition> sstableBounds(long start, long end)
    {
        return bounds(start, true, end, true);
    }
    private static Bounds<PartitionPosition> bounds(long start, boolean startInclusive, long end, boolean endInclusive)
    {
        return new Bounds<>(startInclusive ? t(start).minKeyBound() : t(start).maxKeyBound(),
                            endInclusive ? t(end).maxKeyBound() : t(end).minKeyBound());
    }

}
