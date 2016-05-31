/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public String cfname;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    private boolean initialized;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_COUNTER),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDARD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            LOCAL = FBUtilities.getBroadcastAddress();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
        }

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singleton(tmd.partitioner.getRandomToken()));
        tmd.updateNormalToken(tmd.partitioner.getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        List<InetAddress> expected = new ArrayList<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }

        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddress().getCanonicalHostName(),expected.get(0).getCanonicalHostName());

       assertEquals(expected.get(0), ActiveRepairService.getNeighbors(KEYSPACE5,
               StorageService.instance.getLocalRanges(KEYSPACE5).iterator().next(),
               null, hosts).iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        ActiveRepairService.getNeighbors(KEYSPACE5, StorageService.instance.getLocalRanges(KEYSPACE5).iterator().next(), null, hosts);
    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(tmd.partitioner.getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    @Test
    public void testGetActiveRepairedSSTableRefs()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = store.getLiveSSTables();

        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, 0, false);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        prs.markSSTablesRepairing(store.metadata.cfId, prsId);

        //retrieve all sstable references from parent repair sessions
        Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId);
        Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
        assertEquals(original, retrieved);
        refs.release();

        //remove 1 sstable from data data tracker
        Set<SSTableReader> newLiveSet = new HashSet<>(original);
        Iterator<SSTableReader> it = newLiveSet.iterator();
        final SSTableReader removed = it.next();
        it.remove();
        store.getTracker().dropSSTables(new com.google.common.base.Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader reader)
            {
                return removed.equals(reader);
            }
        }, OperationType.COMPACTION, null);

        //retrieve sstable references from parent repair session again - removed sstable must not be present
        refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId);
        retrieved = Sets.newHashSet(refs.iterator());
        assertEquals(newLiveSet, retrieved);
        assertFalse(retrieved.contains(removed));
        refs.release();
    }

    @Test
    public void testAddingMoreSSTables()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, System.currentTimeMillis(), true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        prs.markSSTablesRepairing(store.metadata.cfId, prsId);
        try (Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
            assertEquals(original, retrieved);
        }
        createSSTables(store, 2);
        boolean exception = false;
        try
        {
            UUID newPrsId = UUID.randomUUID();
            ActiveRepairService.instance.registerParentRepairSession(newPrsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, System.currentTimeMillis(), true);
            ActiveRepairService.instance.getParentRepairSession(newPrsId).markSSTablesRepairing(store.metadata.cfId, newPrsId);
        }
        catch (Throwable t)
        {
            exception = true;
        }
        assertTrue(exception);

        try (Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
            assertEquals(original, retrieved);
        }
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.truncateBlocking();
        store.disableAutoCompaction();
        createSSTables(store, 10);
        return store;
    }

    private void createSSTables(ColumnFamilyStore cfs, int count)
    {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(cfs.metadata, timestamp, Integer.toString(j))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }
    }
}
