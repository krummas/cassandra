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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

public class ForceDropCompactStorageTest extends TestBaseImpl
{
    @Test
    public void forceDropCompactStorageStaticCompact() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, v int, PRIMARY KEY (id)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            Object[][] expected = cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            cluster.schemaChange("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
        }
    }

    @Test
    public void forceDropCompactStorageDense() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);
            Object[][] expected = cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            cluster.schemaChange("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
            cluster.forEach(i -> i.flush(KEYSPACE));
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
        }
    }

    @Test
    public void forceDropCompactStorageMixedStaticCompact() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, v1 int) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).flush(KEYSPACE);
            Object[][] expected = cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
            assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
            cluster.get(2).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
        }
    }

    @Test
    public void forceDropCompactDenseStorageMixed() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);
            Object[][] expected = cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");

            cluster.forEach(i -> i.flush(KEYSPACE));
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
            assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), expected);
        }
    }
    @Test
    public void forceDropCompactDenseStorageMixedWrites() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            for (int i = 5; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);

            for (int i = 0; i < 10; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            for (int i = 0; i < 10; i++)
                assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            cluster.get(2).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");

            for (int i = 0; i < 10; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            cluster.forEach((i) -> i.flush(KEYSPACE));

            for (int i = 0; i < 10; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

        }
    }

    @Test
    public void forceDropCompactDenseStorageMixedWritesAndReadRepair() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);
            Object[][] expected = cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            for (int i = 5; i < 10; i++)
                cluster.get(1).executeInternal("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", i, i);

            for (int i = 0; i < 10; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            cluster.get(2).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");

            for (int i = 0; i < 10; i++)
                assertRows(cluster.get(1).executeInternal("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", i), row(i, i));

            cluster.forEach((i) -> i.flush(KEYSPACE));

            for (int i = 0; i < 10; i++)
                assertRows(cluster.get(2).executeInternal("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", i), row(i, i));

        }
    }

    @Test
    public void forceDropCompactStorageMixedStaticCompactWrites() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, v1 int) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", ConsistencyLevel.ALL, i, i);

            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");

            for (int i = 5; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", ConsistencyLevel.ALL, i, i);

            for (int i = 10; i < 15; i++)
                cluster.coordinator(2).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", ConsistencyLevel.ALL, i, i);

            for (int i = 0; i < 15; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            for (int i = 0; i < 15; i++)
            {
                assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE id = ?", i), row(i, i));
                assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE id = ?", i), row(i, i));
            }
        }
    }

    @Test
    public void forceDropCompactStorageMixedStaticCompactWritesReadRepair() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int PRIMARY KEY, v1 int) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", ConsistencyLevel.ALL, i, i);

            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");

            for (int i = 5; i < 10; i++)
                cluster.get(1).executeInternal("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", i, i);

            for (int i = 10; i < 15; i++)
                cluster.get(2).executeInternal("INSERT INTO "+KEYSPACE+".tbl (id, v1) values (?, ?)", i, i);

            for (int i = 0; i < 15; i++)
                assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = ?", ConsistencyLevel.ALL, i), row(i, i));

            for (int i = 0; i < 15; i++)
            {
                assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE id = ?", i), row(i, i));
                assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE id = ?", i), row(i, i));
            }
        }
    }

    @Test
    public void testRowDeletionDense() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, primary key (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            cluster.coordinator(1).execute("DELETE FROM "+KEYSPACE+".tbl WHERE id = 1 AND ck = 1", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).forceCompact(KEYSPACE, "tbl");

            assertEquals(4, cluster.coordinator(1).execute("select * from "+KEYSPACE+".tbl", ConsistencyLevel.ALL).length);
        }
    }

    @Test
    public void testDeletionStaticCompact() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, i int, primary key (id)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, i) values (?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl FORCE DROP COMPACT STORAGE");
            cluster.coordinator(1).execute("DELETE FROM "+KEYSPACE+".tbl WHERE id = 1", ConsistencyLevel.ALL);
            assertEquals(4, cluster.coordinator(1).execute("select * from "+KEYSPACE+".tbl", ConsistencyLevel.ALL).length);
        }
    }
}
