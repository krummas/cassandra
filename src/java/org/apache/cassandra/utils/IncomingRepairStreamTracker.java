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

package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class IncomingRepairStreamTracker
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingRepairStreamTracker.class);
    private final Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences;

    private IncomingRepairStreamTracker(Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences)
    {
        this.differences = differences;
    }

    private final Map<Range<Token>, Set<Set<InetAddress>>> incoming = new HashMap<>();

    private void addIncomingRangeFrom(Range<Token> range, InetAddress streamFromNode)
    {
        logger.trace("adding incoming range {} from {}", range, streamFromNode);
        Set<Range<Token>> newInput = denormalize(range, incoming);
        assertNonOverLapping(incoming.keySet());

        for (Range<Token> input : newInput)
        {
            if (incoming.containsKey(input))
            {
                if (!maybeAddToExisting(input, streamFromNode, incoming.get(input)))
                    incoming.get(input).add(Sets.newHashSet(streamFromNode));
            }
            else
            {
                incoming.put(input, newSet(streamFromNode));
            }
        }
    }

    /**
     * "Denormalizes" (kind of the opposite of what Range.normalize does) the ranges in the keys of {{incoming}}
     *
     * It makes sure that if there is an intersection between {{range}} and some of the ranges in {{incoming.keySet()}}
     * we know that all intersections are keys in the updated {{incoming}}
     */
    @VisibleForTesting
    static Set<Range<Token>> denormalize(Range<Token> range, Map<Range<Token>, Set<Set<InetAddress>>> incoming)
    {
        logger.trace("Denormalizing range={} incoming={}", range, incoming);
        Set<Range<Token>> existingRanges = new HashSet<>(incoming.keySet());
        Map<Range<Token>, Set<Set<InetAddress>>> existingOverlappingRanges = new HashMap<>();
        for (Range<Token> existingRange : existingRanges)
        {
            if (range.intersects(existingRange))
                existingOverlappingRanges.put(existingRange, incoming.remove(existingRange));
        }

        Set<Range<Token>> intersections = intersection(existingRanges, range);
        Set<Range<Token>> newExisting = Sets.union(subtractFromAllRanges(existingOverlappingRanges.keySet(), range), intersections);
        Set<Range<Token>> newInput = Sets.union(range.subtractAll(existingOverlappingRanges.keySet()), intersections);
        assertNonOverLapping(newExisting);
        assertNonOverLapping(newInput);
        for (Range<Token> r : newExisting)
        {
            for (Map.Entry<Range<Token>, Set<Set<InetAddress>>> entry : existingOverlappingRanges.entrySet())
            {
                if (r.intersects(entry.getKey()))
                    incoming.put(r, copySet(entry.getValue()));
            }
        }
        logger.trace("denormalized {} to {}", range, newInput);
        logger.trace("denormalized incoming to {}", incoming);
        return newInput;
    }

    @VisibleForTesting
    static Set<Range<Token>> subtractFromAllRanges(Collection<Range<Token>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Range<Token> r : ranges)
            result.addAll(r.subtract(range));
        return result;
    }

    private static void assertNonOverLapping(Set<Range<Token>> ranges)
    {
        List<Range<Token>> sortedRanges = Range.sort(ranges);
        Token lastToken = null;
        for (Range<Token> range : sortedRanges)
        {
            if (lastToken != null && lastToken.compareTo(range.left) > 0)
            {
                throw new AssertionError("Ranges are overlapping: "+ranges);
            }
            lastToken = range.right;
        }
    }

    private static Set<Range<Token>> intersection(Collection<Range<Token>> ranges, Range<Token> range)
    {
        Set<Range<Token>> result = new HashSet<>();
        for (Range<Token> r : ranges)
            result.addAll(range.intersectionWith(r));
        return result;
    }

    private static <T> Set<Set<T>> newSet(T streamFrom)
    {
        Set<Set<T>> ss = Sets.newHashSet();
        ss.add(Sets.newHashSet(streamFrom));
        return ss;
    }

    private static <T> Set<Set<T>> copySet(Set<Set<T>> input)
    {
        Set<Set<T>> ret = Sets.newHashSet();
        for (Set<T> s : input)
            ret.add(Sets.newHashSet(s));
        return ret;
    }

    private boolean maybeAddToExisting(Range<Token> r, InetAddress streamFromNode, Set<Set<InetAddress>> sets)
    {
        for (Set<InetAddress> existing : sets)
        {
            // the nodes in 'existing' are all equal for 'r'
            // if 'streamFromNode' is also equal for 'range' - we can just add it to the set
            // and later pick a single node in 'existing' to stream from.
            assert existing.size() > 0;
            InetAddress first = existing.iterator().next();
            if (remoteNodesEqual(r, first, streamFromNode))
            {
                existing.add(streamFromNode);
                return true;
            }
        }
        return false;
    }

    private boolean remoteNodesEqual(Range<Token> range, InetAddress i, InetAddress j)
    {
        List<Range<Token>> diffs = differences.getOrDefault(i, Maps.newHashMap()).get(j); // todo: also check [j][i] if someone calls this method with flipped arguments
        if (diffs != null)
        {
            for (Range<Token> diff : diffs)
            {
                // if the other node has a diff for this range, we know they are not equal.
                if (range.equals(diff) || range.intersects(diff))
                    return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    Map<Range<Token>, Set<Set<InetAddress>>> rawRangesToFetch()
    {
        return ImmutableMap.copyOf(incoming);
    }

    public String toString()
    {
        return "IncomingStreamTracker{" +
               "incoming=" + incoming +
               '}';
    }

    /**
     * Basic idea is that we track incoming ranges instead of blindly just exchanging the ranges that mismatch between two nodes
     *
     * Say node X has tracked that it will stream range r1 from node Y. Now we see find a diffing range
     * r1 between node X and Z. When adding r1 from Z as an incoming to X we check if Y and Z are equal on range r (ie, there is
     * no difference between them). If they are equal X can stream from Y or Z and the end result will be the same.
     *
     * The ranges wont match perfectly since we don't iterate over leaves so we always split based on the
     * smallest range (either the new difference or the existing one)
     */
    @VisibleForTesting
    static Map<InetAddress, IncomingRepairStreamTracker> reduceDifferences(Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences)
    {
        Map<InetAddress, IncomingRepairStreamTracker> trackers = new HashMap<>();
        for (Map.Entry<InetAddress, Map<InetAddress, List<Range<Token>>>> diffs : differences.entrySet())
        {
            for (Map.Entry<InetAddress, List<Range<Token>>> nodeDiffs : diffs.getValue().entrySet())
            {
                for (Range<Token> r : nodeDiffs.getValue())
                {
                    getTracker(differences, trackers, diffs.getKey()).addIncomingRangeFrom(r, nodeDiffs.getKey());
                    getTracker(differences, trackers, nodeDiffs.getKey()).addIncomingRangeFrom(r, diffs.getKey());
                }
            }
        }
        return trackers;
    }

    public static Reduced reduce(Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences, PreferedNodeFilter filter)
    {
        return new Reduced(reduceDifferences(differences), filter);
    }

    private static IncomingRepairStreamTracker getTracker(Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences, Map<InetAddress, IncomingRepairStreamTracker> trackers, InetAddress i)
    {
        if (!trackers.containsKey(i))
            trackers.put(i,  new IncomingRepairStreamTracker(differences));
        return trackers.get(i);
    }

    public static class Reduced
    {
        private final Map<InetAddress, Map<InetAddress, List<Range<Token>>>> reducedMap;

        public Reduced(Map<InetAddress, IncomingRepairStreamTracker> incomingTrackers, PreferedNodeFilter filter)
        {
            reducedMap = reduce(incomingTrackers, filter);
        }

        public Map<InetAddress, List<Range<Token>>> streamsFor(InetAddress node)
        {
            return reducedMap.get(node);
        }

        private static Map<InetAddress, Map<InetAddress, List<Range<Token>>>> reduce(Map<InetAddress, IncomingRepairStreamTracker> incomingTrackers, PreferedNodeFilter filter)
        {
            Map<InetAddress, Integer> outgoingStreamCounts = new HashMap<>();
            Map<InetAddress, Map<InetAddress, List<Range<Token>>>> retMap = new HashMap<>();

            for (Map.Entry<InetAddress, IncomingRepairStreamTracker> trackerEntry : incomingTrackers.entrySet())
            {
                IncomingRepairStreamTracker tracker = trackerEntry.getValue();
                Map<InetAddress, List<Range<Token>>> rangesToFetch = new HashMap<>();
                for (Map.Entry<Range<Token>, Set<Set<InetAddress>>> entry : tracker.incoming.entrySet())
                {
                    Range<Token> rangeToFetch = entry.getKey();
                    for (InetAddress remoteNode : pickLeastStreaming(trackerEntry.getKey(), entry.getValue(), outgoingStreamCounts, filter))
                    {
                        rangesToFetch.computeIfAbsent(remoteNode, k -> new ArrayList<>());
                        rangesToFetch.get(remoteNode).add(rangeToFetch);
                    }
                }
                retMap.put(trackerEntry.getKey(), rangesToFetch);

            }
            return retMap;
        }

        // greedily pick the nodes doing the least amount of streaming
        private static Collection<InetAddress> pickLeastStreaming(InetAddress streamingNode, Set<Set<InetAddress>> values, Map<InetAddress, Integer> outgoingStreamCounts, PreferedNodeFilter filter)
        {
            Set<InetAddress> retSet = new HashSet<>();
            for (Set<InetAddress> toStream : values)
            {
                InetAddress candidate = null;
                Set<InetAddress> prefered = filter.apply(streamingNode, toStream);
                for (InetAddress node : prefered)
                {
                    if (candidate == null || outgoingStreamCounts.getOrDefault(candidate, 0) > outgoingStreamCounts.getOrDefault(node, 0))
                    {
                        candidate = node;
                    }
                }

                if (candidate == null)
                {
                    for (InetAddress node : toStream)
                    {
                        if (candidate == null || outgoingStreamCounts.getOrDefault(candidate, 0) > outgoingStreamCounts.getOrDefault(node, 0))
                        {
                            candidate = node;
                        }
                    }
                }
                assert candidate != null;
                outgoingStreamCounts.put(candidate, outgoingStreamCounts.getOrDefault(candidate, 0) + 1);
                retSet.add(candidate);
            }
            return retSet;
        }
    }

    public static interface PreferedNodeFilter<T>
    {
        public Set<T> apply(T streamingNode, Set<T> toStream);
    }
}




