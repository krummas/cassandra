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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.service.StorageService;

/**
 * Created by blakeeggleston on 11/13/17.
 */
public class HintingReadRepair extends AbstractReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    public HintingReadRepair(ReadCommand command,
                              List<InetAddress> endpoints,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        super(command, endpoints, queryStartNanoTime, consistency);
    }



    public static class PartitionRepair extends ReadRepair.PartitionRepair
    {
        public void reportMutation(InetAddress endpoint, Mutation mutation)
        {
            logger.debug("Writing a hint {} for {}", mutation, endpoint);
            // todo: actually think about gcgs etc
            HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(endpoint), Hint.create(mutation, System.currentTimeMillis()));
        }

        public void finish()
        {}
    }

    public void awaitRepairs(long timeout)
    {
    }

    public PartitionRepair startPartitionRepair()
    {
        return new PartitionRepair();
    }
}
