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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AsyncRepairCallback;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.service.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair extends AbstractReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final List<PartitionRepair> repairs = new ArrayList<>();

    public BlockingReadRepair(ReadCommand command,
                              List<InetAddress> endpoints,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        super(command, endpoints, queryStartNanoTime, consistency);
    }

    public static class PartitionRepair extends ReadRepair.PartitionRepair
    {
        List<AsyncOneResponse<?>> responses;
        public PartitionRepair(int expectedResponses)
        {
            responses = new ArrayList<>(expectedResponses);
        }

        private AsyncOneResponse sendMutation(InetAddress endpoint, Mutation mutation)
        {
            // use a separate verb here because we don't want these to be get the white glove hint-
            // on-timeout behavior that a "real" mutation gets
            Tracing.trace("Sending read-repair-mutation to {}", endpoint);
            MessageOut<Mutation> msg = mutation.createMessage(MessagingService.Verb.READ_REPAIR);
            return MessagingService.instance().sendRR(msg, endpoint);
        }

        public void reportMutation(InetAddress endpoint, Mutation mutation)
        {
            responses.add(sendMutation(endpoint, mutation));
        }

        public void finish()
        {
            Futures.addCallback(Futures.allAsList(responses), new FutureCallback<List<Object>>()
            {
                public void onSuccess(@Nullable List<Object> result)
                {
                    set(result);
                }

                public void onFailure(Throwable t)
                {
                    setException(t);
                }
            });
        }
    }

    public void awaitRepairs(long timeout)
    {
        try
        {
            Futures.allAsList(repairs).get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex)
        {
            // We got all responses, but timed out while repairing
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ReadRepair.PartitionRepair startPartitionRepair()
    {
        PartitionRepair repair = new PartitionRepair(endpoints.size());
        repairs.add(repair);
        return repair;
    }
}
