/*
 *
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
 *
 */
package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class Murmur3PartitionerTest extends PartitionerTestCase<LongToken>
{
    public void initPartitioner()
    {
        partitioner = new Murmur3Partitioner();
    }

    @Override
    protected void midpointMinimumTestCase()
    {
        LongToken mintoken = partitioner.getMinimumToken();
        assert mintoken.compareTo(partitioner.midpoint(mintoken, mintoken)) != 0;
        assertMidpoint(mintoken, tok("a"), 16);
        assertMidpoint(mintoken, tok("aaa"), 16);
        assertMidpoint(mintoken, mintoken, 62);
        assertMidpoint(tok("a"), mintoken, 16);
    }

    @Test
    public void testParts()
    {
        TestPartitioner tp = new TestPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        long left = 0;
        for(int i = 0; i < 100 ; i++)
        {
            long right = left + (Math.abs(FBUtilities.threadLocalRandom().nextLong()) % 100);
            ranges.add(new Range<Token>(new LongToken(left), new LongToken(right)));
            left = right + (Math.abs(FBUtilities.threadLocalRandom().nextLong())%100);
        }
        ranges = new ArrayList<>();
        ranges.add(new Range<Token>(new LongToken(-9223372036854775808L), new LongToken(-9223372036854775808L)));
        System.out.println(ranges);
        System.out.println(tp.getOwnerships(ranges, 2));
    } 
    private static class TestPartitioner
    {
        private static final long MAXIMUM = Long.MAX_VALUE;
        public List<Token> getOwnerships(Collection<Range<Token>> localRanges, int parts)
        {
            assert localRanges.size() > 0;
            // figure out how many tokens each part should have:
            List<Token> boundaryTokens = new ArrayList<>();
            BigInteger allLocalTokens = BigInteger.ZERO;
            for (Range<Token> r : localRanges)
                allLocalTokens = allLocalTokens.add(rangeWidth(r));
            BigInteger tokensPerPart = allLocalTokens.divide(BigInteger.valueOf(parts));
            System.out.println(tokensPerPart);
            // find the boundaries
            Iterator<Range<Token>> rangeIterator = localRanges.iterator();
            Range<Token> curRange = rangeIterator.next();
            BigInteger curRangeWidth = rangeWidth(curRange);
            BigInteger remainingInPart = tokensPerPart;
            BigInteger left = BigInteger.valueOf(((LongToken)curRange.left).token);
            while (boundaryTokens.size() < parts - 1)
            {
                while (remainingInPart.compareTo(curRangeWidth) > 0)
                {
                    remainingInPart = remainingInPart.subtract(curRangeWidth);
                    curRange = rangeIterator.next();
                    curRangeWidth = rangeWidth(curRange);
                    left = BigInteger.valueOf(((LongToken)curRange.left).token);
                }
                boundaryTokens.add(new LongToken(left.add(remainingInPart).longValue()));
                left = left.add(remainingInPart);
                curRangeWidth = curRangeWidth.subtract(remainingInPart);

            }
            boundaryTokens.add(new LongToken(MAXIMUM));
            return boundaryTokens;
        }
        private static BigInteger rangeWidth(Range<Token> r)
        {
            BigInteger left = BigInteger.valueOf(((LongToken)r.left.getToken()).token);
            BigInteger right = BigInteger.valueOf(((LongToken)r.right.getToken()).token);
            if (left.equals(right))
                return right.abs().add(left.abs());
            return right.subtract(left);
        }
    }
}

