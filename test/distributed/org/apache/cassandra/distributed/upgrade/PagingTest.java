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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class PagingTest extends UpgradeTestBase
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
                       expected.addAll(dump("select distinct pk from " + DistributedTestBase.KEYSPACE + ".tbl WHERE token(pk) > " + Long.MIN_VALUE + " AND token(pk) < " + Long.MAX_VALUE, s));
                   }
               })
        .runAfterNodeUpgrade((cluster, node) -> {
            Thread.sleep(5000); // sometimes one node doesn't have time come up properly?
            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint("127.0.0.1")
                                                                                      .withProtocolVersion(ProtocolVersion.V3)
                                                                                      .build();
                 Session s = c.connect())
            {
                assertEquals(expected,
                             dump("select distinct pk from " + DistributedTestBase.KEYSPACE + ".tbl WHERE token(pk) > " + Long.MIN_VALUE + " AND token(pk) < " + Long.MAX_VALUE,
                                  s));
            }
        })
        .run();
    }

    /**
     * insert 50 partitions with 1000 rows each
     * delete using 10 random range tombstones
     * result before upgrade should be the same as result after upgrade and after major compaction
     * @throws Throwable
     */
    @Test
    public void testPagedReadsWithDeletes() throws Throwable
    {
        List<List<Object>> expected = new ArrayList<>(10);
        new UpgradeTestBase.TestCase()
        .nodes(2)
//        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup((cluster) ->
               {
                   cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) ");
                   ICoordinator coordinator = cluster.coordinator(1);
                   for (int i = 0; i < 10; i++)
                       for (int j = 0; j < 1000; j++)
                           coordinator.execute("insert into " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES ("+i+", " + j + ", "+(i * j)+ ")", ConsistencyLevel.ALL);
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
                   // now write 10 random range tombstones per partition:
                   for (int i = 0; i < 10; i++)
                   {
                       List<Integer> rts = new ArrayList<>(20);
                       Random r = new Random();
                       for (int j = 0; j < 20; j++)
                           rts.add(r.nextInt(1000));
                       Collections.sort(rts);
                       for (int j = 0; j < 20; j+=2)
                       {
                           String query = String.format("DELETE FROM %s.%s WHERE pk = %d and ck > %d and ck < %d", DistributedTestBase.KEYSPACE, "tbl", i, rts.get(j), rts.get(j+1));
                           cluster.coordinator(1).execute(query, ConsistencyLevel.ALL);
                       }
                   }
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
                   expected.clear();
                   try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                             .addContactPoint("127.0.0.1")
                                                                                             .withProtocolVersion(ProtocolVersion.V3)
                                                                                             .build();
                        Session s = c.connect())
                   {
                       for (int i = 0; i < 10; i++)
                           expected.addAll(dump("SELECT pk, ck, v FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk=" + i, s));
                   }

               })
        .runAfterNodeUpgrade((cluster, node) -> {
            long start = System.currentTimeMillis();
            Thread.sleep(1000); // sometimes one node doesn't have time come up properly?
            List<List<Object>> results = new ArrayList<>();
            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint("127.0.0.1")
                                                                                      .build();
                 Session s = c.connect())
            {
                for (int j = 0; j < 50; j++)
                    results.addAll(dump("SELECT pk, ck, v FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk=" + j, s));
                assertEquals(expected, results);
                cluster.get(node).forceCompact(DistributedTestBase.KEYSPACE, "tbl");
                results.clear();
                for (int j = 0; j < 50; j++)
                    results.addAll(dump("SELECT pk, ck, v FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk=" + j, s));
                assertEquals(expected, results);
            }
            System.out.println("XYZ afterNodeUpgrade: "+(System.currentTimeMillis() - start));
        })
        .run();
    }

    /**
     * tests paged reads with range tombstones
     * node1 is made mismatching, and that node is upgraded first
     *
     * we then read the non-mismatching node with ALL to trigger a cross-version RR
     */
    @Test
    public void testPagedReadsWithDeletesReadRepairNewMismatchNode_readMismatching() throws Throwable
    {
        testPagedReadsWithDeletesReadRepairHelper(1, false);
    }

    /**
     * tests paged reads with range tombstones
     * node1 is made mismatching, and that node is upgraded first
     *
     * we then read the mismatching node with ALL to trigger a cross-version RR
     */
    @Test
    public void testPagedReadsWithDeletesReadRepairNewMismatchNode_readMatching() throws Throwable
    {
        testPagedReadsWithDeletesReadRepairHelper(1, true);
    }

    /**
     * tests paged reads with range tombstones
     * node2 is made mismatching, and that node is upgraded last
     *
     * we then read the non-mismatching node with ALL to trigger a cross-version RR
     */
    @Test
    public void testPagedReadsWithDeletesReadRepairOldMismatchNode_readMismatching() throws Throwable
    {
        testPagedReadsWithDeletesReadRepairHelper(2, false);
    }

    /**
     * tests paged reads with range tombstones
     * node2 is made mismatching, and that node is upgraded last
     *
     * we then read the mismatching node with ALL to trigger a cross-version RR
     */
    @Test
    public void testPagedReadsWithDeletesReadRepairOldMismatchNode_readMatching() throws Throwable
    {
        testPagedReadsWithDeletesReadRepairHelper(2, true);
    }

    public void testPagedReadsWithDeletesReadRepairHelper(int mismatchNode, boolean readMismatchingNode) throws Throwable
    {
        int upNode = 3 - mismatchNode;
        new UpgradeTestBase.TestCase()
        .nodes(2)
//        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                    .set("hinted_handoff_enabled", false))
        .setup((cluster) ->
               {
                   cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) ");
                   for (int j = 0; j < 1000; j++)
                       cluster.coordinator(1).execute("insert into " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, " + j + ", " + j + ")", ConsistencyLevel.ALL);
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
                   String query = String.format("DELETE FROM %s.%s WHERE pk = %d and ck > %d and ck < %d", DistributedTestBase.KEYSPACE, "tbl", 1, 50, 250);
                   cluster.get(upNode).executeInternal(query);
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
               })
        .runAfterNodeUpgrade((cluster, node) ->
                             {
                                 String host = "127.0.0."+(readMismatchingNode ? mismatchNode : upNode);
                                 try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                                           .addContactPoint(host)
                                                                                                           .build();
                                      Session s = c.connect())
                                 {
                                     Thread.sleep(1000);
                                     assertRows(s, 175, 51, 25);
                                     cluster.get(node).flush(DistributedTestBase.KEYSPACE);
                                     cluster.get(node).forceCompact(DistributedTestBase.KEYSPACE, "tbl");
                                     assertRows(s, 5000, 801, 25);
                                 }
                             })
        .runAfterClusterUpgrade((cluster) ->
                                {
                                    // major compact all nodes and read
                                    cluster.forEach((node) -> node.forceCompact(DistributedTestBase.KEYSPACE, "tbl"));
                                    try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                                              .addContactPoint("127.0.0.1")
                                                                                                              .build();
                                         Session s = c.connect())
                                    {
                                        Thread.sleep(1000);
                                        Set<Integer> fetchSizes = Sets.newHashSet(10, 49, 50, 51, 249, 250, 251, 799, 800, 801, 1000);
                                        for (int fetchSize : fetchSizes)
                                            assertRows(s, 2000, 801, fetchSize);
                                    }
                                })
        .run();
    }

    private void assertRows(Session s, int ckLessThan, int expectedRows, int fetchSize)
    {
        int i = 0;
        int count = 0;
        String query = "SELECT pk, ck, v FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk=1 and ck < "+ckLessThan;
        for (List<Object> row : dump(query, s, 20))
        {
            // rt: 50 < ck < 250
            if (i == 51)
                i = 250;
            assertEquals("Mismatched row with fetchSize=" + fetchSize,
                         row,
                         Lists.newArrayList(1, i, i));
            i++;
            count++;
        }
        assertEquals(expectedRows, count);
    }

    private List<List<Object>> dump(String query, Session s, int fetchSize)
    {
        List<List<Object>> result = new ArrayList<>(50);
        Statement stmt = new SimpleStatement(query);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        stmt.setFetchSize(fetchSize);
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

    private List<List<Object>> dump(String query, Session s)
    {
        return dump(query, s, 103);
    }

}
