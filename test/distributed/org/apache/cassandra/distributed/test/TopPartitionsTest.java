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

package org.apache.cassandra.distributed.test;

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.psjava.util.AssertStatus.assertTrue;


public class TopPartitionsTest extends TestBaseImpl
{
    @Test
    public void basicPartitionSizeTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck))");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, ck, t) values (?,?,?)", ConsistencyLevel.ALL, i, j, i * j + 100);

            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                // partitions 99 -> 90 are the largest, make sure they are in the map;
                Map<String, Long> sizes = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopSizePartitions();
                for (int i = 99; i >= 90; i--)
                    assertTrue(sizes.containsKey(String.valueOf(i)));

                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                assertEquals(10, tombstones.size());
                assertTrue(tombstones.values().stream().allMatch(l -> l == 0));
            });
        }
    }

    @Test
    public void basicRowTombstonesTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 1");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // tombstones not purgeable
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            Thread.sleep(2000);
            // count purgeable tombstones;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            // all tombstones actually purged;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
            });
        }
    }

    @Test
    public void basicRegularTombstonesTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 1");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET t = null where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // tombstones not purgeable
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            Thread.sleep(2000);
            // count purgeable tombstones;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });

            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            // all tombstones actually purged;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTopTombstonePartitions();
                assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
            });
        }
    }
}
