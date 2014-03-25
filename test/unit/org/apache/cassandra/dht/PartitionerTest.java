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
package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import org.apache.cassandra.utils.Pair;
import static org.junit.Assert.assertEquals;

public class PartitionerTest
{
//    @Test
//    public void testSplitRange()
//    {
//        List<Pair<BigInteger, BigInteger>> ranges = new ArrayList<>();
//        ranges.add(Pair.create(BigInteger.valueOf(0), BigInteger.valueOf(100)));
//        List<BigInteger> split = AbstractPartitioner.rangeSplitHelper(ranges, 3, new RandomPartitioner());
//        assertEquals(3, split.size());
//        assertEquals(BigInteger.valueOf(33), split.get(0));
//        assertEquals(BigInteger.valueOf(66), split.get(1));
//        assertEquals(BigInteger.valueOf(2).pow(127), split.get(2));
//    }
//
//    @Test
//    public void testSplitRanges()
//    {
//        List<Pair<BigInteger, BigInteger>> ranges = new ArrayList<>();
//        ranges.add(Pair.create(BigInteger.valueOf(0), BigInteger.valueOf(100)));
//        ranges.add(Pair.create(BigInteger.valueOf(200), BigInteger.valueOf(300)));
//        List<BigInteger> split = AbstractPartitioner.rangeSplitHelper(ranges, 2, new RandomPartitioner());
//        assertEquals(2, split.size());
//        assertEquals(BigInteger.valueOf(100), split.get(0));
//    }
//    @Test
//    public void testSplitRangesNegative()
//    {
//        List<Pair<BigInteger, BigInteger>> ranges = new ArrayList<>();
//        ranges.add(Pair.create(BigInteger.valueOf(-200), BigInteger.valueOf(-100)));
//        ranges.add(Pair.create(BigInteger.valueOf(-50), BigInteger.valueOf(50)));
//        List<BigInteger> split = AbstractPartitioner.rangeSplitHelper(ranges, 3, new RandomPartitioner());
//        assertEquals(3, split.size());
//        // -100 + 200 + (50 + 50) = 200 total ranges owned, 66 per part
//        assertEquals(BigInteger.valueOf(-200+66), split.get(0));
//        assertEquals(BigInteger.valueOf(-200+50/*(gap)*/+66+66), split.get(1));
//    }
//    @Test
//    public void testSplitRangesFull()
//    {
//        List<Pair<BigInteger, BigInteger>> ranges = new ArrayList<>();
//        ranges.add(Pair.create(BigInteger.valueOf(-200), BigInteger.valueOf(-200)));
//        List<BigInteger> split = AbstractPartitioner.rangeSplitHelper(ranges, 3, new RandomPartitioner());
//        assertEquals(3, split.size());
//        assertEquals(BigInteger.valueOf(2).pow(127).divide(BigInteger.valueOf(3)), split.get(0));
//        assertEquals(BigInteger.valueOf(2).pow(127).multiply(BigInteger.valueOf(2)).divide(BigInteger.valueOf(3)), split.get(1));
//        assertEquals(BigInteger.valueOf(2).pow(127), split.get(2));
//    }
//    @Test
//    public void testSplitRangesFullM3P()
//    {
//        List<Pair<BigInteger, BigInteger>> ranges = new ArrayList<>();
//        ranges.add(Pair.create(BigInteger.valueOf(-200), BigInteger.valueOf(-200)));
//        List<BigInteger> split = AbstractPartitioner.rangeSplitHelper(ranges, 3, new Murmur3Partitioner());
//        assertEquals(3, split.size());
//        BigInteger allTokens = BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.valueOf(Long.MIN_VALUE));
//        assertEquals(BigInteger.valueOf(Long.MIN_VALUE).add(allTokens.divide(BigInteger.valueOf(3))), split.get(0));
//        assertEquals(BigInteger.valueOf(Long.MIN_VALUE).add(allTokens.multiply(BigInteger.valueOf(2)).divide(BigInteger.valueOf(3))), split.get(1));
//        assertEquals(BigInteger.valueOf(Long.MIN_VALUE).add(allTokens), split.get(2));
//    }

}
