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

import org.apache.cassandra.config.DatabaseDescriptor;
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


    /**
     * Split the partitioner total range
     *
     * @param parts the number of parts to split the range in
     * @return a list with the boundaries for the parts, last boundary will be max token.
     */
    public List<T> splitRanges(int parts)
    {
        if (DatabaseDescriptor.getNumTokens() > 1 && parts > 0)
        {
            if(parts == 1)
                return Arrays.asList(getMaximumToken());

            List<BigInteger> boundaries = new ArrayList<>(parts);
            BigInteger partWidth = totalRangeWidth().divide(BigInteger.valueOf(parts));
            boundaries.add(minTokenValue().add(partWidth));
            for (int i = 1; i < parts - 1; i++)
            {
                boundaries.add(boundaries.get(i - 1).add(partWidth));
            }
            List<T> tokenBoundaries = new ArrayList<>(parts);
            for (BigInteger boundary : boundaries)
            {
                tokenBoundaries.add(tokenForValue(boundary));
            }
            tokenBoundaries.add(getMaximumToken());
            return tokenBoundaries;
        }
        return null;
    }

    protected BigInteger totalRangeWidth()
    {
        throw new UnsupportedOperationException("Range width not implemented for "+this.getClass().getName());
    }

    protected BigInteger minTokenValue()
    {
        throw new UnsupportedOperationException("Min not implemented for "+this.getClass().getName());
    }

    protected T tokenForValue(BigInteger value)
    {
        return null;
    }

    public abstract T getMaximumToken();

}
