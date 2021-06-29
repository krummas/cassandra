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

package org.apache.cassandra.db.compaction;


import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class ScheduledCompactionsRandomPartitionerTest
{
    private static final String KEYSPACE1 = "LeveledCompactionStrategyTest";
    private static final String CF_STANDARDDLEVELED_SCHEDULED = "StandardLeveledScheduled";

    private Keyspace keyspace;
    private ColumnFamilyStore cfsScheduled;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setPartitionerUnsafe(RandomPartitioner.instance);
        SchemaLoader.prepareServer();
        Map<String, String> scheduledOpts = new HashMap<>();
        scheduledOpts.put("sstable_size_in_mb", "1");
        scheduledOpts.put("scheduled_compactions", "true");
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED_SCHEDULED)
                                                .compaction(CompactionParams.lcs(scheduledOpts)));
    }

    @Test
    public void testWrapWithRP() throws Exception
    {
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(t(1)),
                                                                      FBUtilities.getBroadcastAddress());
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(t(200)),
                                                                      InetAddress.getByName("127.0.0.2"));
        keyspace = Keyspace.open(KEYSPACE1);
        cfsScheduled = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED_SCHEDULED);
        LeveledCompactionStrategyTest.testGetScheduledCompaction(100, cfsScheduled);
        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    private Token t(long t)
    {
        return new RandomPartitioner.BigIntegerToken(BigInteger.valueOf(t));
    }
}
