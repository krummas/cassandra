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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeStreamerTest extends CQLTester
{
    @Before
    public void before() throws Throwable
    {
        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort  endpoint) {return "R1";}

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                InetAddress address = endpoint.address;
                char c = address.toString().charAt(address.toString().length() - 1);
                if (c <= '3')
                    return "datacenter1";
                if (c <= '6')
                    return "datacenter2";
                return "datacenter3";
            }

            public List<InetAddressAndPort > getSortedListByProximity(InetAddressAndPort  address, Collection<InetAddressAndPort > unsortedAddress) { return null;}
            public void sortByProximity(InetAddressAndPort  address, List<InetAddressAndPort > addresses) {}
            public int compareEndpoints(InetAddressAndPort  target, InetAddressAndPort  a1, InetAddressAndPort  a2) {return 0;}
            public void gossiperStarting(){}
            public boolean isWorthMergingForRangeQuery(List<InetAddressAndPort > merged, List<InetAddressAndPort > l1, List<InetAddressAndPort > l2){return false;}
        });
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort > hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        Util.createInitialRing(StorageService.instance, Murmur3Partitioner.instance, endpointTokens, keyTokens, hosts, hostIds, 9);
    }

    /**
     * Test that the endpoint filtering works
     *
     */
    @Test
    public void testBootstrapCLPotentialEndpoints() throws UnknownHostException
    {
        List<InetAddressAndPort > allEndpoints = new ArrayList<>();
        for (int i = 1; i <= 9; i++)
            allEndpoints.add(InetAddressAndPort .getByName("127.0.0."+i));
        // local dc
        assertEquals(Lists.newArrayList(InetAddressAndPort.getByName("127.0.0.1"),
                                        InetAddressAndPort.getByName("127.0.0.2"),
                                        InetAddressAndPort.getByName("127.0.0.3")),
                     RangeStreamer.getPotentialCLEndpoints(allEndpoints, ConsistencyLevel.LOCAL_QUORUM, Collections.emptySet()));
        // no filtering and not dclocal
        assertEquals(allEndpoints, RangeStreamer.getPotentialCLEndpoints(allEndpoints, ConsistencyLevel.QUORUM, Collections.emptySet()));
        // avoid localhost
        RangeStreamer.ISourceFilter localFilter = new RangeStreamer.ExcludeLocalNodeFilter();
        assertEquals(Lists.newArrayList(InetAddressAndPort.getByName("127.0.0.2"),
                                        InetAddressAndPort.getByName("127.0.0.3")),
                     RangeStreamer.getPotentialCLEndpoints(allEndpoints, ConsistencyLevel.LOCAL_QUORUM, Sets.newHashSet(localFilter)));

    }

    /**
     * Make sure we fetch each range from 2 different endpoints, with more ranges than endpoints
     */
    @Test
    public void testBootstrapCL() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        for (int i = 0; i < 10; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            for (int j = 1; j <= 9; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
        }

        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> clEndpoints = RangeStreamer.getEndpointsForConsistencyLevel(cl,
                                                                                                               Keyspace.open(keyspace()),
                                                                                                               rangesForKs,
                                                                                                               rangeFetchMap,
                                                                                                               "testing",
                                                                                                               Collections.emptySet());
        checkEndpoints(clEndpoints, 2, true, 10);
    }

    /**
     * Make sure we fetch each range from 2 different endpoints, with fewer ranges than endpoints
     */
    @Test
    public void testBootstrapCLFewRanges() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            for (int j = 1; j <= 9; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
        }

        Multimap<InetAddressAndPort, Range<Token>> clEndpoints = RangeStreamer.getEndpointsForConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM, Keyspace.open(keyspace()), rangesForKs, rangeFetchMap, "testing", Collections.emptySet());
        checkEndpoints(clEndpoints, 2, true, 3);
    }


    /**
     * Make sure we fetch each range from 2 different endpoints, with fewer ranges than endpoints
     */
    @Test
    public void testBootstrapQuorumCLFewRanges() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            for (int j = 1; j <= 9; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
        }

        Multimap<InetAddressAndPort, Range<Token>> clEndpoints = RangeStreamer.getEndpointsForConsistencyLevel(ConsistencyLevel.QUORUM, Keyspace.open(keyspace()), rangesForKs, rangeFetchMap, "testing", Collections.emptySet());
        checkEndpoints(clEndpoints, 5, false, 3);
    }

    private void checkEndpoints(Multimap<InetAddressAndPort, Range<Token>> clEndpoints, int expectedEndpointCount, boolean localDc, int rangesToFind)
    {
        Multimap<Range<Token>, InetAddressAndPort> rangeToEP = HashMultimap.create();
        assertTrue(clEndpoints.values().stream().allMatch(r -> r instanceof RangeStreamer.UnrepairedRange));
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : clEndpoints.entries())
        {
            if (localDc)
                assertTrue(ConsistencyLevel.ANY.isLocal(entry.getKey()));
            rangeToEP.put(entry.getValue(), entry.getKey());
        }

        int foundRanges = 0;
        for (Range<Token> r : rangeToEP.keySet())
        {
            assertEquals(expectedEndpointCount, new HashSet<>(rangeToEP.get(r)).size());
            foundRanges++;
        }
        assertEquals(rangesToFind, foundRanges);
    }

    /**
     * Prepopulate the rangeFetchMap and make sure we count it towards the CL and that we fetch all
     * ranges from 127.0.0.3 (we filter localhost away, and we are streaming all ranges from 127.0.0.2)
     */
    @Test
    public void testBootstrapCLNonEmptyFetchMap() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            for (int j = 1; j <= 9; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
            // assume we are fetching everything from 127.0.0.2:
            rangeFetchMap.put(InetAddressAndPort.getByName("127.0.0.2"), r);
        }
        // avoid the local node:
        Multimap<InetAddressAndPort, Range<Token>> clEndpoints = RangeStreamer.getEndpointsForConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM,
                                                                                                        Keyspace.open(keyspace()),
                                                                                                        rangesForKs,
                                                                                                        rangeFetchMap,
                                                                                                        "testing",
                                                                                                        Sets.newHashSet(new RangeStreamer.ExcludeLocalNodeFilter()));
        // no localhost fetching:
        assertFalse(clEndpoints.containsKey(InetAddressAndPort.getByName("127.0.0.1")));
        // we stream everything from 127.0.0.2, don't add any cl endpoints from that host
        assertFalse(clEndpoints.containsKey(InetAddressAndPort.getByName("127.0.0.2")));
        // all unrepaired should come from 127.0.0.3:
        assertEquals(3, new HashSet<>(clEndpoints.get(InetAddressAndPort.getByName("127.0.0.3"))).size());
        assertTrue(clEndpoints.get(InetAddressAndPort.getByName("127.0.0.3")).stream().allMatch(r -> r instanceof RangeStreamer.UnrepairedRange));
    }

    /**
     * Test with non-local quorum
     */
    @Test
    public void testBootstrapQuorumNonEmptyFetchMap() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            for (int j = 1; j <= 9; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
            // assume we are fetching everything from 127.0.0.2:
            rangeFetchMap.put(InetAddressAndPort.getByName("127.0.0.2"), r);
        }
        // avoid the local node:
        Multimap<InetAddressAndPort, Range<Token>> clEndpoints = RangeStreamer.getEndpointsForConsistencyLevel(ConsistencyLevel.QUORUM,
                                                                                                        Keyspace.open(keyspace()),
                                                                                                        rangesForKs,
                                                                                                        rangeFetchMap,
                                                                                                        "testing",
                                                                                                        Sets.newHashSet(new RangeStreamer.ExcludeLocalNodeFilter()));
        assertFalse(clEndpoints.containsKey(InetAddressAndPort.getByName("127.0.0.1")));
        assertFalse(clEndpoints.containsKey(InetAddressAndPort.getByName("127.0.0.2")));
        assertTrue(clEndpoints.values().stream().allMatch(r -> r instanceof RangeStreamer.UnrepairedRange));
        Multimap<Range<Token>, InetAddressAndPort> rangeToEP = HashMultimap.create();
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : clEndpoints.entries())
            rangeToEP.put(entry.getValue(), entry.getKey());

        int foundRanges = 0;
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            assertEquals(4, new HashSet<>(rangeToEP.get(r)).size()); // 4 (quorum = 5) because we are fetching one copy from 127.0.0.2 already
            foundRanges++;
        }
        assertEquals(3, foundRanges);
    }

    /**
     * if the range is only available on 4 nodes, make sure bootstrapping with QUORUM fails.
     * @throws Throwable
     */
    @Test(expected = IllegalStateException.class)
    public void testBootstrapCLFailed() throws Throwable
    {
        execute("ALTER KEYSPACE " + keyspace() + " WITH replication = {'class':'NetworkTopologyStrategy', 'datacenter1':3, 'datacenter2':3, 'datacenter3':3}");
        Multimap<Range<Token>, InetAddressAndPort> rangesForKs = HashMultimap.create();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = HashMultimap.create();
        for (int i = 0; i < 3; i++)
        {
            Range<Token> r = new Range<>(new Murmur3Partitioner.LongToken(i), new Murmur3Partitioner.LongToken(i + 1));
            // only 4 nodes contain the ranges, we will not be able to achieve QUORUM
            for (int j = 1; j <= 4; j++)
                rangesForKs.put(r, InetAddressAndPort.getByName("127.0.0." + j));
            // assume we are fetching everything from 127.0.0.2:
            rangeFetchMap.put(InetAddressAndPort.getByName("127.0.0.2"), r);
        }
        RangeStreamer.getEndpointsForConsistencyLevel(ConsistencyLevel.QUORUM,
                                                      Keyspace.open(keyspace()),
                                                      rangesForKs,
                                                      rangeFetchMap,
                                                      "testing",
                                                      Sets.newHashSet(new RangeStreamer.ExcludeLocalNodeFilter()));
    }
}
