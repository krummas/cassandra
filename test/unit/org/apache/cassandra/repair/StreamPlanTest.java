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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamTransferTask;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class StreamPlanTest extends CQLTester
{
    @Before
    public void before() throws Throwable
    {
        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddress endpoint) {return "R1";}

            public String getDatacenter(InetAddress endpoint)
            {
                char c = endpoint.toString().charAt(endpoint.toString().length() - 1);
                if (c <= '2')
                    return "dc1";
                if (c <= '4')
                    return "dc2";
                return "dc3";
            }

            public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress) { return null;}
            public void sortByProximity(InetAddress address, List<InetAddress> addresses) {}
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) {return 0;}
            public void gossiperStarting(){}
            public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2){return false;}
        });
        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        Util.createInitialRing(StorageService.instance, Murmur3Partitioner.instance, endpointTokens, keyTokens, hosts, hostIds, 6);

        createTable("create table %s (id int primary key, val text)");
        schemaChange("alter keyspace "+getCurrentColumnFamilyStore().keyspace.getName()+" with replication = {'class':'NetworkTopologyStrategy', 'readonly_dcs':'dc2,dc3', 'dc1':3, 'dc2':3, 'dc3':3}");
        for (int i = 0; i < 100; i++)
            execute("insert into %s (id, val) values ("+i+", 'uhuhu')");
        getCurrentColumnFamilyStore().forceBlockingFlush();
    }
    @Test
    public void localSyncTaskTestReadableDCs() throws Throwable
    {

        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        Range<Token> range = new Range<>(sstable.first.getToken(), sstable.last.getToken());
        UUID parentRepairSession = registerParentRepairSession(range);
        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), getCurrentColumnFamilyStore().keyspace.getName(), getCurrentColumnFamilyStore().getTableName(), Collections.singletonList(range));

        assertStreams(desc, range, "127.0.0.1", "127.0.0.1", true, true);
        assertStreams(desc, range, "127.0.0.1", "127.0.0.3", false, true);
        assertStreams(desc, range, "127.0.0.2", "127.0.0.1", true, true);
        assertStreams(desc, range, "127.0.0.2", "127.0.0.4", false, true);
        assertStreams(desc, range, "127.0.0.3", "127.0.0.1", true, false);
        assertStreams(desc, range, "127.0.0.4", "127.0.0.5", false, false);
    }

    private void assertStreams(RepairJobDesc desc, Range<Token> range, String source, String dest, boolean incomingStreams, boolean outgoingStreams) throws UnknownHostException
    {
        TreeResponse r1 = new TreeResponse(InetAddress.getByName(source), null);
        TreeResponse r2 = new TreeResponse(InetAddress.getByName(dest), null);
        LocalSyncTask task = new LocalSyncTask(desc, r1, r2, ActiveRepairService.NO_PENDING_REPAIR, false, PreviewKind.NONE, Sets.newHashSet("dc2", "dc3"));
        StreamPlan sp = task.createStreamPlan(InetAddress.getByName(source), r2.endpoint, r2.endpoint, Collections.singletonList(range));
        Pair<Collection<StreamRequest>, Collection<StreamTransferTask>> streams = sp.getRequestsAndTransfers();
        assertEquals(incomingStreams, streams.left.size() > 0);
        assertEquals(outgoingStreams, streams.right.size() > 0);
    }

    private UUID registerParentRepairSession(Range<Token> range)
    {
        UUID parentRepairSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddress(),
                                                                 Collections.singletonList(getCurrentColumnFamilyStore()), Collections.singletonList(range), false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false,
                                                                 PreviewKind.NONE);
        return parentRepairSession;
    }

    @Test
    public void testStreamingRepairTaskReadonlyDCs() throws UnknownHostException
    {
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        Range<Token> range = new Range<>(sstable.first.getToken(), sstable.last.getToken());
        UUID parentRepairSession = registerParentRepairSession(range);

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), getCurrentColumnFamilyStore().keyspace.getName(), getCurrentColumnFamilyStore().getTableName(), Collections.singletonList(range));
        assertRemoteStreams(desc, range, "127.0.0.2", "127.0.0.1", true, true);
        assertRemoteStreams(desc, range, "127.0.0.1", "127.0.0.2", true, true);
        assertRemoteStreams(desc, range, "127.0.0.2", "127.0.0.3", false, true);
        assertRemoteStreams(desc, range, "127.0.0.3", "127.0.0.2", true, false);
        assertRemoteStreams(desc, range, "127.0.0.3", "127.0.0.5", false, false);


    }
    private void assertRemoteStreams(RepairJobDesc desc, Range<Token> range, String source, String dest, boolean streamToSrc, boolean streamToDst) throws UnknownHostException
    {
        InetAddress src = InetAddress.getByName(source);
        InetAddress dst = InetAddress.getByName(dest);
        SyncRequest sr = new SyncRequest(desc,
                                         FBUtilities.getBroadcastAddress(),
                                         src,
                                         dst,
                                         Collections.singletonList(range),
                                         PreviewKind.NONE);
        StreamingRepairTask task = new StreamingRepairTask(desc, sr, null, PreviewKind.NONE);
        StreamPlan sp = task.createStreamPlan(src, dst, dst);
        Pair<Collection<StreamRequest>, Collection<StreamTransferTask>> streams = sp.getRequestsAndTransfers();

        assertEquals(streamToSrc, streams.left.size() > 0);
        assertEquals(streamToDst, streams.right.size() > 0);
    }

}
