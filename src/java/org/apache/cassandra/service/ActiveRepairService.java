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
package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.*;
import org.apache.cassandra.repair.messages.AnticompactionRequest;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService();

    public static final long UNREPAIRED_SSTABLE = 0;

    private static final ThreadPoolExecutor executor;
    static
    {
        executor = new JMXConfigurableThreadPoolExecutor(4,
                                                         60,
                                                         TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         new NamedThreadFactory("AntiEntropySessions"),
                                                         "internal");
    }

    public static enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions;

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions;

    private CountDownLatch prepareLatch = null;
    /**
     * Protected constructor. Use ActiveRepairService.instance.
     */
    protected ActiveRepairService()
    {
        sessions = new ConcurrentHashMap<>();
        parentRepairSessions = new ConcurrentHashMap<>();
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairFuture submitRepairSession(Map<String, UUID> parentRepairSessions, Range<Token> range, String keyspace, boolean isSequential, Set<InetAddress> endpoints, String... cfnames)
    {
        RepairSession session = new RepairSession(parentRepairSessions, range, keyspace, isSequential, endpoints, cfnames);
        if (session.endpoints.isEmpty())
            return null;
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    public void addToActiveSessions(RepairSession session)
    {
        sessions.put(session.getId(), session);
        Gossiper.instance.register(session);
        FailureDetector.instance.registerFailureDetectionEventListener(session);
    }

    public void removeFromActiveSessions(RepairSession session)
    {
        FailureDetector.instance.unregisterFailureDetectionEventListener(session);
        Gossiper.instance.unregister(session);
        sessions.remove(session.getId());
    }

    public void terminateSessions()
    {
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown();
        }
    }

    // for testing only. Create a session corresponding to a fake request and
    // add it to the sessions (avoid NPE in tests)
    RepairFuture submitArtificialRepairSession(RepairJobDesc desc)
    {
        Map<String, UUID> psid = new HashMap<>();
        psid.put(desc.columnFamily, desc.parentSessionId);
        RepairSession session = new RepairSession(psid, desc.sessionId, desc.range, desc.keyspace, false, null, new String[]{desc.columnFamily});
        sessions.put(session.getId(), session);
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Range<Token> toRepair, Collection<String> dataCenters)
    {
        if (dataCenters != null && !dataCenters.contains(DatabaseDescriptor.getLocalDataCenter()))
            throw new IllegalArgumentException("The local data center must be part of the repair");

        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (dataCenters != null)
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }

        return neighbors;
    }

    public UUID prepareForRepair(ColumnFamilyStore cfs, Set<InetAddress> endpoints, Collection<Range<Token>> ranges, boolean fullRepair)
    {
        UUID parentRepairSession = UUIDGen.getTimeUUID();
        registerParentRepairSession(parentRepairSession, cfs, ranges, fullRepair);
        prepareLatch = new CountDownLatch(endpoints.size());
        IAsyncCallback callback = new IAsyncCallback()
        {
            @Override
            public void response(MessageIn msg)
            {
                ActiveRepairService.this.prepareLatch.countDown();
            }

            @Override
            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };

        UUID cfId = cfs.metadata.cfId;
        for(InetAddress neighbour : endpoints)
        {
            PrepareMessage message = new PrepareMessage(parentRepairSession, cfId, ranges, fullRepair);
            MessageOut<RepairMessage> msg = message.createMessage();
            MessagingService.instance().sendRR(msg, neighbour, callback);
        }
        try
        {
            prepareLatch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return parentRepairSession;
    }

    public void registerParentRepairSession(UUID parentRepairSession, ColumnFamilyStore cfs, Collection<Range<Token>> ranges, boolean fullRepair)
    {
        Set<SSTableReader> sstablesToRepair = new HashSet<>();
        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (new Bounds<>(sstable.first.token, sstable.last.token).intersects(ranges))
            {
                if ((fullRepair || !sstable.isRepaired()))
                {
                    sstablesToRepair.add(sstable);
                }
            }
        }
        parentRepairSessions.put(parentRepairSession, new ParentRepairSession(cfs, ranges, fullRepair, sstablesToRepair, System.currentTimeMillis()));
    }

    public void finishParentSessions(Collection<UUID> parentSessions, Set<InetAddress> neighbors) throws InterruptedException, ExecutionException, IOException
    {

        for (UUID parentSessionId : parentSessions)
        {
            for (InetAddress neighbor : neighbors)
            {
                AnticompactionRequest acr = new AnticompactionRequest(parentSessionId);
                MessageOut<RepairMessage> req = acr.createMessage();
                MessagingService.instance().sendOneWay(req, neighbor);
            }
            doAntiCompaction(parentSessionId);
        }
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.get(parentSessionId);
    }

    public void doAntiCompaction(UUID parentRepairSession) throws InterruptedException, ExecutionException, IOException
    {
        if (getParentRepairSession(parentRepairSession).fullRepair)
        {
            logger.info("Doing full repair - will not do anticompaction.");
            parentRepairSessions.remove(parentRepairSession);
            return;
        }

        Collection<SSTableReader> sstables = null;
        try
        {
            ParentRepairSession prs = getParentRepairSession(parentRepairSession);
            sstables = new HashSet<>(prs.getAndReferenceSSTables());
            boolean success = false;
            while (!success)
            {
                for (SSTableReader compactingSSTable : prs.cfs.getDataTracker().getCompacting())
                {
                    if (sstables.remove(compactingSSTable))
                        SSTableReader.releaseReferences(Arrays.asList(compactingSSTable));
                }
                success = sstables.isEmpty() || prs.cfs.getDataTracker().markCompacting(sstables);
            }

            CompactionManager.instance.performAnticompaction(prs.cfs, prs.ranges, sstables, prs.repairedAt);
        }
        finally
        {
            if (sstables != null)
                SSTableReader.releaseReferences(sstables);
            parentRepairSessions.remove(parentRepairSession);
        }
    }

    public void clearParentRepairSessions()
    {
        this.parentRepairSessions.clear();
    }


    public void handleMessage(InetAddress endpoint, RepairMessage message)
    {
        RepairJobDesc desc = message.desc;
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        switch (message.messageType)
        {
            case VALIDATION_COMPLETE:
                ValidationComplete validation = (ValidationComplete) message;
                session.validationComplete(desc, endpoint, validation.tree);
                break;
            case SYNC_COMPLETE:
                // one of replica is synced.
                SyncComplete sync = (SyncComplete) message;
                session.syncComplete(desc, sync.nodes, sync.success);
                break;
            default:
                break;
        }
    }

    public static class ParentRepairSession
    {
        public final ColumnFamilyStore cfs;
        public final Collection<Range<Token>> ranges;
        public final boolean fullRepair;
        public final Collection<SSTableReader> sstables;
        public final long repairedAt;

        public ParentRepairSession(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, boolean fullRepair, Collection<SSTableReader> sstables, long repairedAt)
        {
            this.cfs = cfs;
            this.ranges = ranges;
            this.fullRepair = fullRepair;
            this.sstables = sstables;
            this.repairedAt = repairedAt;
        }

        public Collection<SSTableReader> getAndReferenceSSTables()
        {
            if (fullRepair)
                return cfs.markCurrentSSTablesReferenced();

            Iterator<SSTableReader> sstableIterator = sstables.iterator();
            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                if (!new File(sstable.descriptor.filenameFor(Component.DATA)).exists())
                {
                    sstableIterator.remove();
                }
                else
                {
                    if (!sstable.acquireReference())
                        sstableIterator.remove();
                }
            }
            return sstables;
        }
    }
}
