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

package org.apache.cassandra.distributed.upgrade;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class NetworkPagingTest extends UpgradeTestBase
{
    /**
     * Makes sure we don't get duplicase when doing DISTINCT queries
     */
    @Test
    public void testDistinctReads() throws Throwable
    {
        List<List<Object>> expected = new ArrayList<>();
        new UpgradeTestBase.TestCase()
        .nodes(2)
//        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup((cluster) ->
               {
                   cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) ");
                   for (int j = 0; j < 1000; j++)
                   {
                       for (int i = 0; i < 5; i++)
                           cluster.coordinator(1).execute("insert into " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES ("+j+", " + i + ", "+i*j+")", ConsistencyLevel.ALL);
                   }
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
                   expected.clear();
                   try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                             .addContactPoint("127.0.0.1")
                                                                                             .withProtocolVersion(ProtocolVersion.V3)
                                                                                             .build();
                        Session s = c.connect())
                   {
                       expected.addAll(dump("select distinct pk from " + DistributedTestBase.KEYSPACE + ".tbl WHERE token(pk) > " + Long.MIN_VALUE + " AND token(pk) < " + Long.MAX_VALUE,
                                            s));
                   }
               })
        .runAfterNodeUpgrade((cluster, node) -> {
            try (AbstractCluster<?>.AllMembersAliveMonitor monitor = cluster.allMembersLiveMonitor();
                 com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint("127.0.0.1")
                                                                                      .withProtocolVersion(ProtocolVersion.V3)
                                                                                      .build();
                 Session s = c.connect())
            {
                monitor.waitForCompletion();
                assertEquals(expected,
                             dump("select distinct pk from " + DistributedTestBase.KEYSPACE + ".tbl WHERE token(pk) > " + Long.MIN_VALUE + " AND token(pk) < " + Long.MAX_VALUE,
                                  s));
            }
        })
        .run();
    }
    private List<List<Object>> dump(String query, Session s)
    {
        List<List<Object>> result = new ArrayList<>(50);
        Statement stmt = new SimpleStatement(query);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        stmt.setFetchSize(103);
        ResultSet rs = s.execute(stmt);
        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext())
        {
            Row r = iter.next();
            List<Object> rowRes = new ArrayList<>(r.getColumnDefinitions().size());
            for (int j = 0; j < r.getColumnDefinitions().size(); j++)
                rowRes.add(r.getObject(j));
            result.add(rowRes);
        }
        return result;
    }
}
