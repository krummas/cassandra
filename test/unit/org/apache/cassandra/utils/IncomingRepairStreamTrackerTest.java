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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IncomingRepairStreamTrackerTest
{
    private static final InetAddress[] addresses;
    static
    {
        addresses = new InetAddress[20];
        for (int i = 0; i < 20; i++)
        {
            try
            {
                addresses[i] = InetAddress.getByName("127.0.0." + i);
            }
            catch (UnknownHostException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSimpleReducing()
    {
        /*
        A == B and D == E =>
        A streams from C, {D, E} since D==E
        B streams from C, {D, E} since D==E
        C streams from {A, B}, {D, E} since A==B and D==E
        D streams from {A, B}, C since A==B
        E streams from {A, B}, C since A==B

  A   B   C   D   E
A     =   x   x   x
B         x   x   x
C             x   x
D                 =
         */
        Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            for (int j = i + 1; j < 5; j++)
            {
                List<Range<Token>> diff = list(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(10)));
                differences.computeIfAbsent(addresses[i], k -> new HashMap<>());
                differences.get(addresses[i]).put(addresses[j], diff);
            }
        }
        differences.get(addresses[0]).remove(addresses[1]);
        differences.get(addresses[3]).remove(addresses[4]);
        Map<InetAddress, IncomingRepairStreamTracker> tracker = IncomingRepairStreamTracker.reduceDifferences(differences);
        assertEquals(set(set(addresses[2]), set(addresses[4],addresses[3])), tracker.get(addresses[0]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[2]), set(addresses[4],addresses[3])), tracker.get(addresses[1]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[4],addresses[3])), tracker.get(addresses[2]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[2])), tracker.get(addresses[3]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[2])), tracker.get(addresses[4]).rawRangesToFetch().values().iterator().next());

        IncomingRepairStreamTracker.Reduced reduced = IncomingRepairStreamTracker.reduce(differences, (x,y) -> y);

        Map<InetAddress, List<Range<Token>>> n0 = reduced.streamsFor(addresses[0]);
        assertNull(n0.get(addresses[0]));
        assertNull(n0.get(addresses[1]));
        assertNotNull(n0.get(addresses[2]));
        assertTrue(n0.get(addresses[3]) != null ^ n0.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n1 = reduced.streamsFor(addresses[1]);
        assertNull(n1.get(addresses[0]));
        assertNull(n1.get(addresses[1]));
        assertNotNull(n1.get(addresses[2]));
        assertTrue(n1.get(addresses[3]) != null ^ n1.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n2 = reduced.streamsFor(addresses[2]);
        assertTrue(n2.get(addresses[0]) != null ^ n2.get(addresses[1]) != null);
        assertNull(n2.get(addresses[2]));
        assertTrue(n2.get(addresses[3]) != null ^ n2.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n3 = reduced.streamsFor(addresses[3]);
        assertTrue(n3.get(addresses[0]) != null ^ n3.get(addresses[1]) != null);
        assertNotNull(n3.get(addresses[2]));
        assertNull(n3.get(addresses[3]));
        assertNull(n3.get(addresses[4]));

        Map<InetAddress, List<Range<Token>>> n4 = reduced.streamsFor(addresses[4]);
        assertTrue(n4.get(addresses[0]) != null ^ n4.get(addresses[1]) != null);
        assertNotNull(n4.get(addresses[2]));
        assertNull(n4.get(addresses[3]));
        assertNull(n4.get(addresses[4]));
    }
    @Test
    public void testSimpleReducingWithPreferedNodes()
    {
        /*
        A == B and D == E =>
        A streams from C, {D, E} since D==E
        B streams from C, {D, E} since D==E
        C streams from {A, B}, {D, E} since A==B and D==E
        D streams from {A, B}, C since A==B
        E streams from {A, B}, C since A==B

  A   B   C   D   E
A     =   x   x   x
B         x   x   x
C             x   x
D                 =
         */
        Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            for (int j = i + 1; j < 5; j++)
            {
                List<Range<Token>> diff = list(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(10)));
                differences.computeIfAbsent(addresses[i], k -> new HashMap<>());
                differences.get(addresses[i]).put(addresses[j], diff);
            }
        }
        differences.get(addresses[0]).remove(addresses[1]);
        differences.get(addresses[3]).remove(addresses[4]);

        Map<InetAddress, IncomingRepairStreamTracker> tracker = IncomingRepairStreamTracker.reduceDifferences(differences);
        assertEquals(set(set(addresses[2]), set(addresses[4],addresses[3])), tracker.get(addresses[0]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[2]), set(addresses[4],addresses[3])), tracker.get(addresses[1]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[4],addresses[3])), tracker.get(addresses[2]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[2])), tracker.get(addresses[3]).rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(addresses[0],addresses[1]), set(addresses[2])), tracker.get(addresses[4]).rawRangesToFetch().values().iterator().next());

        // if there is an option, never stream from node 1:
        IncomingRepairStreamTracker.Reduced reduced = IncomingRepairStreamTracker.reduce(differences, (x,y) -> Sets.difference(y, set(addresses[1])));

        Map<InetAddress, List<Range<Token>>> n0 = reduced.streamsFor(addresses[0]);
        assertNull(n0.get(addresses[0]));
        assertNull(n0.get(addresses[1]));
        assertNotNull(n0.get(addresses[2]));
        assertTrue(n0.get(addresses[3]) != null ^ n0.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n1 = reduced.streamsFor(addresses[1]);
        assertNull(n1.get(addresses[0]));
        assertNull(n1.get(addresses[1]));
        assertNotNull(n1.get(addresses[2]));
        assertTrue(n1.get(addresses[3]) != null ^ n1.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n2 = reduced.streamsFor(addresses[2]);
        assertNotNull(n2.get(addresses[0]));
        assertNull(n2.get(addresses[1]));
        assertNull(n2.get(addresses[2]));
        assertTrue(n2.get(addresses[3]) != null ^ n2.get(addresses[4]) != null);

        Map<InetAddress, List<Range<Token>>> n3 = reduced.streamsFor(addresses[3]);
        assertNotNull(n3.get(addresses[0]));
        assertNull(n3.get(addresses[1]));
        assertNotNull(n3.get(addresses[2]));
        assertNull(n3.get(addresses[3]));
        assertNull(n3.get(addresses[4]));

        Map<InetAddress, List<Range<Token>>> n4 = reduced.streamsFor(addresses[4]);
        assertNotNull(n4.get(addresses[0]));
        assertNull(n4.get(addresses[1]));
        assertNotNull(n4.get(addresses[2]));
        assertNull(n4.get(addresses[3]));
        assertNull(n4.get(addresses[4]));
    }

    @Test
    public void testOverlapDifference()
    {
        /*
            |A     |B     |C
         ---+------+------+--------
         A  |=     |50,100|0,50
         B  |      |=     |0,100
         C  |      |      |=

         A needs to stream (50, 100] from B, (0, 50] from C
         B needs to stream (50, 100] from A, (0, 100] from C
         C needs to stream (0, 50] from A, (0, 100] from B
         A == B on (0, 50]   => C can stream (0, 50] from either A or B
         A == C on (50, 100] => B can stream (50, 100] from either A or C
         =>
         A streams (50, 100] from {B}, (0, 50] from C
         B streams (0, 50] from {C}, (50, 100] from {A, C}
         C streams (0, 50] from {A, B}, (50, 100] from B
         */
        Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences = new HashMap<>();
        differences.put(addresses[0], new HashMap<>());
        differences.get(addresses[0]).put(addresses[1], list(range(50, 100)));
        differences.get(addresses[0]).put(addresses[2], list(range(0, 50)));
        differences.put(addresses[1], new HashMap<>());
        differences.get(addresses[1]).put(addresses[2], list(range(0, 100)));
        Map<InetAddress, IncomingRepairStreamTracker> tracker = IncomingRepairStreamTracker.reduceDifferences(differences);
        assertEquals(set(set(addresses[2])), tracker.get(addresses[0]).rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(addresses[1])), tracker.get(addresses[0]).rawRangesToFetch().get(range(50, 100)));
        assertEquals(set(set(addresses[2])), tracker.get(addresses[1]).rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(addresses[0],addresses[2])), tracker.get(addresses[1]).rawRangesToFetch().get(range(50, 100)));
        assertEquals(set(set(addresses[0],addresses[1])), tracker.get(addresses[2]).rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(addresses[1])), tracker.get(addresses[2]).rawRangesToFetch().get(range(50, 100)));

        IncomingRepairStreamTracker.Reduced reduced = IncomingRepairStreamTracker.reduce(differences, (x,y) -> y);

        Map<InetAddress, List<Range<Token>>> n0 = reduced.streamsFor(addresses[0]);

        assertTrue(n0.get(addresses[1]).equals(list(range(50, 100))));
        assertTrue(n0.get(addresses[2]).equals(list(range(0, 50))));

        Map<InetAddress, List<Range<Token>>> n1 = reduced.streamsFor(addresses[1]);
        assertNull(n1.get(addresses[1]));
        if (n1.get(addresses[0]) != null)
        {
            assertTrue(n1.get(addresses[2]).equals(list(range(0, 50))));
            assertTrue(n1.get(addresses[0]).equals(list(range(50, 100))));
        }
        else
        {
            assertTrue(n1.get(addresses[2]).equals(list(range(0, 50), range(50, 100))));
        }
        Map<InetAddress, List<Range<Token>>> n2 = reduced.streamsFor(addresses[2]);
        assertNull(n2.get(addresses[2]));
        if (n2.get(addresses[0]) != null)
        {
            assertTrue(n2.get(addresses[0]).equals(list(range(0,50))));
            assertTrue(n2.get(addresses[1]).equals(list(range(50, 100))));
        }
        else
        {
            assertTrue(n2.get(addresses[0]).equals(list(range(0, 50), range(50, 100))));
        }


    }

    @Test
    public void testOverlapDifference2()
    {
        /*
            |A               |B               |C
         ---+----------------+----------------+------------------
         A  |=               |5,45            |0,10 40,50
         B  |                |=               |0,5 10,40 45,50
         C  |                |                |=

         A needs to stream (5, 45] from B, (0, 10], (40, 50) from C
         B needs to stream (5, 45] from A, (0, 5], (10, 40], (45, 50] from C
         C needs to stream (0, 10], (40,50] from A, (0,5], (10,40], (45,50] from B
         A == B on (0, 5], (45, 50]
         A == C on (10, 40]
         B == C on (5, 10], (40, 45]
         */

        Map<InetAddress, Map<InetAddress, List<Range<Token>>>> differences = new HashMap<>();
        differences.put(addresses[0], new HashMap<>());
        differences.get(addresses[0]).put(addresses[1], list(range(5, 45)));
        differences.get(addresses[0]).put(addresses[2], list(range(0, 10), range(40,50)));
        differences.put(addresses[1], new HashMap<>());
        differences.get(addresses[1]).put(addresses[2], list(range(0, 5), range(10,40), range(45,50)));
        Map<InetAddress, IncomingRepairStreamTracker> tracker = IncomingRepairStreamTracker.reduceDifferences(differences);

        Map<Range<Token>, Set<Set<InetAddress>>> ranges = tracker.get(addresses[0]).rawRangesToFetch();
        assertEquals(5, ranges.size());

        assertEquals(set(set(addresses[2])), ranges.get(range(0, 5)));
        assertEquals(set(set(addresses[1], addresses[2])), ranges.get(range(5, 10)));
        assertEquals(set(set(addresses[1])), ranges.get(range(10, 40)));
        assertEquals(set(set(addresses[1], addresses[2])), ranges.get(range(40, 45)));
        assertEquals(set(set(addresses[2])), ranges.get(range(45, 50)));

        ranges = tracker.get(addresses[1]).rawRangesToFetch();
        assertEquals(5, ranges.size());
        assertEquals(set(set(addresses[2])), ranges.get(range(0, 5)));
        assertEquals(set(set(addresses[0])), ranges.get(range(5, 10)));
        assertEquals(set(set(addresses[0], addresses[2])), ranges.get(range(10, 40)));
        assertEquals(set(set(addresses[0])), ranges.get(range(40, 45)));
        assertEquals(set(set(addresses[2])), ranges.get(range(45, 50)));

        ranges = tracker.get(addresses[2]).rawRangesToFetch();
        assertEquals(5, ranges.size());
        assertEquals(set(set(addresses[0], addresses[1])), ranges.get(range(0, 5)));
        assertEquals(set(set(addresses[0])), ranges.get(range(5, 10)));
        assertEquals(set(set(addresses[1])), ranges.get(range(10, 40)));
        assertEquals(set(set(addresses[0])), ranges.get(range(40, 45)));
        assertEquals(set(set(addresses[0],addresses[1])), ranges.get(range(45, 50)));
        IncomingRepairStreamTracker.Reduced reduced = IncomingRepairStreamTracker.reduce(differences, (x, y) -> y);

        assertNoOverlap(addresses[0], reduced.streamsFor(addresses[0]), list(range(0, 50)));
        assertNoOverlap(addresses[1], reduced.streamsFor(addresses[1]), list(range(0, 50)));
        assertNoOverlap(addresses[2], reduced.streamsFor(addresses[2]), list(range(0, 50)));
    }

    private void assertNoOverlap(InetAddress incomingNode, Map<InetAddress, List<Range<Token>>> node, List<Range<Token>> expectedAfterNormalize)
    {
        Set<Range<Token>> allRanges = new HashSet<>();
        Set<InetAddress> remoteNodes = Sets.newHashSet(addresses[0],addresses[1],addresses[2]);
        remoteNodes.remove(incomingNode);
        Iterator<InetAddress> iter = remoteNodes.iterator();
        allRanges.addAll(node.get(iter.next()));
        InetAddress i = iter.next();
        for (Range<Token> r : node.get(i))
        {
            for (Range<Token> existing : allRanges)
                if (r.intersects(existing))
                    fail();
        }
        allRanges.addAll(node.get(i));
        List<Range<Token>> normalized = Range.normalize(allRanges);
        assertEquals(expectedAfterNormalize, normalized);
    }

    @SafeVarargs
    private static List<Range<Token>> list(Range<Token> r, Range<Token> ... rs)
    {
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(r);
        Collections.addAll(ranges, rs);
        return ranges;
    }

    private static Set<InetAddress> set(InetAddress ... elem)
    {
        return Sets.newHashSet(elem);
    }
    @SafeVarargs
    private static Set<Set<InetAddress>> set(Set<InetAddress> ... elem)
    {
        Set<Set<InetAddress>> ret = Sets.newHashSet();
        ret.addAll(Arrays.asList(elem));
        return ret;
    }

    private Murmur3Partitioner.LongToken longtok(long l)
    {
        return new Murmur3Partitioner.LongToken(l);
    }
    private Range<Token> range(long t, long t2)
    {
        return new Range<>(longtok(t), longtok(t2));
    }

    @Test
    public void testSubtractAllRanges()
    {
        Set<Range<Token>> ranges = new HashSet<>();
        ranges.add(range(10, 20)); ranges.add(range(40, 60));
        assertEquals(0, IncomingRepairStreamTracker.subtractFromAllRanges(ranges, range(0, 100)).size());
        ranges.add(range(90, 110));
        assertEquals(Sets.newHashSet(range(100, 110)), IncomingRepairStreamTracker.subtractFromAllRanges(ranges, range(0, 100)));
        ranges.add(range(-10, 10));
        assertEquals(Sets.newHashSet(range(-10, 0), range(100, 110)), IncomingRepairStreamTracker.subtractFromAllRanges(ranges, range(0, 100)));
    }

    @Test
    public void testDenormalize()
    {
        // test when the new incoming range is fully contained within an existing incoming range
        Set<Set<InetAddress>> dummy = Sets.<Set<InetAddress>>newHashSet(Sets.newHashSet(addresses[1]));
        Map<Range<Token>, Set<Set<InetAddress>>> incoming = new HashMap<>();
        incoming.put(range(0, 100), dummy);
        Set<Range<Token>> newInput = IncomingRepairStreamTracker.denormalize(range(30, 40), incoming);
        assertEquals(3, incoming.size());
        assertTrue(incoming.containsKey(range(0, 30)));
        assertTrue(incoming.containsKey(range(30, 40)));
        assertTrue(incoming.containsKey(range(40, 100)));
        assertEquals(1, newInput.size());
        assertTrue(newInput.contains(range(30, 40)));
    }

    @Test
    public void testDenormalize2()
    {
        // test when the new incoming range fully contains an existing incoming range
        Set<Set<InetAddress>> dummy = Sets.<Set<InetAddress>>newHashSet(Sets.newHashSet(addresses[1]));
        Map<Range<Token>, Set<Set<InetAddress>>> incoming = new HashMap<>();
        incoming.put(range(40, 50), dummy);
        Set<Range<Token>> newInput = IncomingRepairStreamTracker.denormalize(range(0, 100), incoming);
        assertEquals(1, incoming.size());
        assertTrue(incoming.containsKey(range(40, 50)));
        assertEquals(3, newInput.size());
        assertTrue(newInput.contains(range(0, 40)));
        assertTrue(newInput.contains(range(40, 50)));
        assertTrue(newInput.contains(range(50, 100)));
    }

    @Test
    public void testDenormalize3()
    {
        // test when there are multiple existing incoming ranges and the new incoming overlaps some and contains some
        Set<Set<InetAddress>> dummy = Sets.<Set<InetAddress>>newHashSet(Sets.newHashSet(addresses[1]));
        Map<Range<Token>, Set<Set<InetAddress>>> incoming = new HashMap<>();
        incoming.put(range(0, 100), dummy);
        incoming.put(range(200, 300), dummy);
        incoming.put(range(500, 600), dummy);
        Set<Range<Token>> expectedNewInput = Sets.newHashSet(range(50, 100), range(100, 200), range(200, 300), range(300, 350));
        Set<Range<Token>> expectedIncomingKeys = Sets.newHashSet(range(0, 50), range(50, 100), range(200, 300), range(500, 600));
        Set<Range<Token>> newInput = IncomingRepairStreamTracker.denormalize(range(50, 350), incoming);
        assertEquals(expectedNewInput, newInput);
        assertEquals(expectedIncomingKeys, incoming.keySet());
    }
}
