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


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IncomingRepairStreamTrackerTest
{
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
        List<Range<Token>>[][] differences = new List[5][5];
        for (int i = 0; i < differences.length - 1; i++)
        {
            for (int j = i + 1; j < differences.length; j++)
            {
                differences[i][j] = Lists.newArrayList(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(10)));
            }
        }
        differences[0][1] = null;
        differences[3][4] = null;
        IncomingRepairStreamTracker [] tracker = IncomingRepairStreamTracker.reduceDifferences(differences);
        assertEquals(set(set(2), set(4,3)), tracker[0].rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(2), set(4,3)), tracker[1].rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(0,1), set(4,3)), tracker[2].rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(0,1), set(2)), tracker[3].rawRangesToFetch().values().iterator().next());
        assertEquals(set(set(0,1), set(2)), tracker[4].rawRangesToFetch().values().iterator().next());
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
        List<Range<Token>>[][] differences = new List[3][3];
        differences[0][1] = Lists.newArrayList(range(50, 100));
        differences[0][2] = Lists.newArrayList(range(0, 50));
        differences[1][2] = Lists.newArrayList(range(0, 100));
        IncomingRepairStreamTracker [] tracker = IncomingRepairStreamTracker.reduceDifferences(differences);
        assertEquals(set(set(2)), tracker[0].rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(1)), tracker[0].rawRangesToFetch().get(range(50, 100)));
        assertEquals(set(set(2)), tracker[1].rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(0,2)), tracker[1].rawRangesToFetch().get(range(50, 100)));
        assertEquals(set(set(0,1)), tracker[2].rawRangesToFetch().get(range(0, 50)));
        assertEquals(set(set(1)), tracker[2].rawRangesToFetch().get(range(50, 100)));
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
        List<Range<Token>>[][] differences = new List[3][3];
        differences[0][1] = Lists.newArrayList(range(5, 45));
        differences[0][2] = Lists.newArrayList(range(0, 10), range(40,50));
        differences[1][2] = Lists.newArrayList(range(0, 5), range(10,40), range(45,50));
        IncomingRepairStreamTracker [] tracker = IncomingRepairStreamTracker.reduceDifferences(differences);

        assertEquals(5, tracker[0].rawRangesToFetch().size());
        Map<Range<Token>, Set<Set<Integer>>> ranges = tracker[0].rawRangesToFetch();

        assertEquals(set(set(2)), ranges.get(range(0, 5)));
        assertEquals(set(set(1, 2)), ranges.get(range(5, 10)));
        assertEquals(set(set(1)), ranges.get(range(10, 40)));
        assertEquals(set(set(1,2)), ranges.get(range(40, 45)));
        assertEquals(set(set(2)), ranges.get(range(45, 50)));

        ranges = tracker[1].rawRangesToFetch();
        assertEquals(5, ranges.size());
        assertEquals(set(set(2)), ranges.get(range(0, 5)));
        assertEquals(set(set(0)), ranges.get(range(5, 10)));
        assertEquals(set(set(0,2)), ranges.get(range(10, 40)));
        assertEquals(set(set(0)), ranges.get(range(40, 45)));
        assertEquals(set(set(2)), ranges.get(range(45, 50)));

        ranges = tracker[2].rawRangesToFetch();
        assertEquals(5, ranges.size());
        assertEquals(set(set(0, 1)), ranges.get(range(0, 5)));
        assertEquals(set(set(0)), ranges.get(range(5, 10)));
        assertEquals(set(set(1)), ranges.get(range(10, 40)));
        assertEquals(set(set(0)), ranges.get(range(40, 45)));
        assertEquals(set(set(0,1)), ranges.get(range(45, 50)));

    }

    private static Set<Integer> set(Integer ... elem)
    {
        return Sets.newHashSet(elem);
    }
    @SafeVarargs
    private static Set<Set<Integer>> set(Set<Integer> ... elem)
    {
        Set<Set<Integer>> ret = Sets.newHashSet();
        for (Set<Integer> i : elem)
            ret.add(i);
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
    public void oeu()
    {
        Set<Range<Token>> ranges = new HashSet<>();
        ranges.add(range(10, 20)); ranges.add(range(40, 60));
        System.out.println(range(0, 100).subtractAll(ranges));

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
        Set<Set<Integer>> dummy = Sets.<Set<Integer>>newHashSet(Sets.newHashSet(1));
        Map<Range<Token>, Set<Set<Integer>>> incoming = new HashMap<>();
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
        Set<Set<Integer>> dummy = Sets.<Set<Integer>>newHashSet(Sets.newHashSet(1));
        Map<Range<Token>, Set<Set<Integer>>> incoming = new HashMap<>();
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
        Set<Set<Integer>> dummy = Sets.<Set<Integer>>newHashSet(Sets.newHashSet(1));
        Map<Range<Token>, Set<Set<Integer>>> incoming = new HashMap<>();
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
