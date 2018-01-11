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

package org.apache.cassandra.service.reads.repair;

import java.net.InetAddress;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AsyncRepairCallback;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.service.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

public abstract class AbstractReadRepair implements ReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadRepair.class);

    protected final ReadCommand command;
    protected final List<InetAddress> endpoints;
    protected final long queryStartNanoTime;
    protected final ConsistencyLevel consistency;

    private volatile DigestRepair digestRepair = null;

    AbstractReadRepair(ReadCommand command, List<InetAddress> endpoints, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        this.command = command;
        this.endpoints = endpoints;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
    }

    static class DigestRepair
    {
        private final DataResolver dataResolver;
        private final ReadCallback readCallback;
        private final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<PartitionIterator> resultConsumer)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.resultConsumer = resultConsumer;
        }
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddress[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddress> allEndpoints, List<InetAddress> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, allEndpoints.size(), queryStartNanoTime, this);
        ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, contactedEndpoints.size(), command,
                                                     keyspace, allEndpoints, queryStartNanoTime, this);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

        for (InetAddress endpoint : contactedEndpoints)
        {
            Tracing.trace("Enqueuing full data read to {}", endpoint);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), endpoint, readCallback);
        }
    }

    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {
        if (digestRepair != null)
        {
            digestRepair.readCallback.awaitResults();
            digestRepair.resultConsumer.accept(digestRepair.dataResolver.resolve());
        }
    }

    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {
        TraceState traceState = Tracing.instance.get();
        if (traceState != null)
            traceState.trace("Initiating read-repair");
        StageManager.getStage(Stage.READ_REPAIR).execute(() -> resolver.evaluateAllResponses(traceState));
    }

    public void backgroundDigestRepair(TraceState traceState)
    {
        if (traceState != null)
            traceState.trace("Digest mismatch");
        if (logger.isDebugEnabled())
            logger.debug("Digest mismatch");

        ReadRepairMetrics.repairedBackground.mark();

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver repairResolver = new DataResolver(keyspace, command, consistency, endpoints.size(), queryStartNanoTime, this);
        AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRR(command.createMessage(), endpoint, repairHandler);
    }
}
