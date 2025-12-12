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

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DisableMetadataChangesTest extends TestBaseImpl
{
    @Test
    public void testDisableAlterSchema() throws IOException
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).start()))
        {
            runSchemaChanges(cluster, "should_succeed", false);
            cluster.forEach(i -> i.runOnInstance(() -> StorageService.instance.setSchemaModificationsDisabled(true)));
            cluster.schemaChange(withKeyspace("create keyspace IF NOT EXISTS %s with replication = { 'class': 'SimpleStrategy', 'replication_factor':  1 } "));
            cluster.schemaChange(withKeyspace("create table IF NOT EXISTS %s.tbl_should_succeed (id int primary key)"));
            runSchemaChanges(cluster, "should_fail", true);
            cluster.forEach(i -> i.runOnInstance(() -> StorageService.instance.setSchemaModificationsDisabled(false)));
        }
    }

    private void runSchemaChanges(Cluster cluster, String name, boolean expectFailure)
    {
        String [] changes = new String[] {
        "create keyspace %s with replication = { 'class': 'SimpleStrategy', 'replication_factor': 2}",
        withKeyspace("create table %s.tbl_%%s (id int primary key)"),
        withKeyspace("alter table %s.tbl_should_succeed with comment = 'blabla %%s'"),
        withKeyspace("alter keyspace %s with durable_writes=" + expectFailure)
        };
        for (String cql : changes)
        {
            try
            {
                cluster.schemaChange(String.format(cql, name));
                assertFalse(expectFailure);
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("Schema modifications are currently disabled"));
                assertTrue(e.getMessage(), expectFailure);
            }
        }
    }

    @Test
    public void testDisableNodetoolCommands() throws IOException
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).start()))
        {
            cluster.forEach(i -> i.runOnInstance(() -> StorageService.instance.setRingChangesDisabled(true)));
            NodeToolResult res = cluster.get(1).nodetoolResult("move", "1");
            res.asserts().failure();
            assertTrue(res.getStdout().contains("Ring changes are disabled"));

            res = cluster.get(1).nodetoolResult("decommission");
            res.asserts().failure();
            assertTrue(res.getStdout().contains("Ring changes are disabled"));

            res = cluster.get(1).nodetoolResult("removenode", "123");
            res.asserts().failure();
            assertTrue(res.getStdout(), res.getStdout().contains("ring changes are disabled"));

            cluster.forEach(i -> i.runOnInstance(() -> StorageService.instance.setRingChangesDisabled(false)));
        }
    }
}
