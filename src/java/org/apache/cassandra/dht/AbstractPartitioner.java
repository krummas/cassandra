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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.utils.Pair;

public abstract class AbstractPartitioner<T extends Token> implements IPartitioner<T>
{
    public <R extends RingPosition> R minValue(Class<R> klass)
    {
        Token minToken = getMinimumToken();
        if (minToken.getClass().equals(klass))
            return (R)minToken;
        else
            return (R)minToken.minKeyBound();
    }

    public List<T> splitRanges(List<Range<T>> localRanges, int parts)
    {
        return Arrays.asList(getMaximumToken());
    }

    public static List<BigInteger> rangeSplitHelper(List<Pair<BigInteger, BigInteger>> ranges, int parts, AbstractPartitioner<? extends Token> partitioner)
    {
        List<BigInteger> boundaryTokens = new ArrayList<>(parts);
        BigInteger allLocalTokens = BigInteger.ZERO;
        for (Pair<BigInteger, BigInteger> range : ranges)
        {
            allLocalTokens = allLocalTokens.add(width(range, partitioner));
        }
        BigInteger tokensPerPart = allLocalTokens.divide(BigInteger.valueOf(parts));
        Iterator<Pair<BigInteger, BigInteger>> rangeIterator = ranges.iterator();
        Pair<BigInteger, BigInteger> curRange = rangeIterator.next();
        BigInteger curRangeWidth = width(curRange, partitioner);
        BigInteger remainingInPart = tokensPerPart;
        BigInteger left = curRange.left;
        if (curRange.left.equals(curRange.right))
            left = partitioner.minTokenValue();
        while (boundaryTokens.size() < parts - 1)
        {
            while (remainingInPart.compareTo(curRangeWidth) > 0)
            {
                remainingInPart = remainingInPart.subtract(curRangeWidth);
                curRange = rangeIterator.next();
                curRangeWidth = width(curRange, partitioner);
                left = curRange.left;
            }
            boundaryTokens.add(left.add(remainingInPart));
            left = left.add(remainingInPart);
            curRangeWidth = curRangeWidth.subtract(remainingInPart);
        }
        boundaryTokens.add(partitioner.maxTokenValue());
        return boundaryTokens;
    }

    public static BigInteger width(Pair<BigInteger, BigInteger> range, AbstractPartitioner<? extends Token> partitioner)
    {
        if (range.left.equals(range.right))
            return partitioner.totalRangeWidth();
        return range.right.subtract(range.left);
    }

    protected BigInteger totalRangeWidth()
    {
        throw new UnsupportedOperationException("Range width not implemented for "+this.getClass().getName());
    }

    protected BigInteger minTokenValue()
    {
        throw new UnsupportedOperationException("Min not implemented for "+this.getClass().getName());
    }

    protected BigInteger maxTokenValue()
    {
        throw new UnsupportedOperationException("Max not implemented for "+this.getClass().getName());
    }

    protected abstract T getMaximumToken();

}
