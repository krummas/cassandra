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

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;

public class NoopReadRepair implements ReadRepair
{
    public static final NoopReadRepair instance = new NoopReadRepair();

    private NoopReadRepair() {}

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddress[] endpoints)
    {
        return UnfilteredPartitionIterators.MergeListener.NOOP;
    }

    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddress> allEndpoints, List<InetAddress> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        resultConsumer.accept(digestResolver.getData());
    }

    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {

    }

    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {

    }

    public void backgroundDigestRepair(TraceState traceState)
    {

    }

    @Override
    public PartitionRepair startPartitionRepair()
    {
        return new NoopPartitionRepair();
    }

    @Override
    public void awaitRepairs(long timeout)
    {
    }

    private static class NoopPartitionRepair extends PartitionRepair
    {
        @Override
        public void reportMutation(InetAddress endpoint, Mutation mutation)
        {
        }

        @Override
        public void finish()
        {
        }
    }
}
