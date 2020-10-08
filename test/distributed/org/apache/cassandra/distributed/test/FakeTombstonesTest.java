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

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;


public class FakeTombstonesTest extends TestBaseImpl
{
    @Test
    public void rrFakeTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck > 10 AND ck < 20");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck = 15", ConsistencyLevel.ALL);

            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Row[info=[ts=-9223372036854775808] del=deletedAt=1602162729030000, localDeletion=1602162729 ]: ck=15 |
                 */
            });
        }
    }

    /**
     *
     * node1 RRs a row from an sstable on node1
     *
     */
    @Test
    public void rrFake2Test() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck > 10 AND ck < 20");
            cluster.get(2).flush(KEYSPACE);
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck = 15", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker INCL_START_BOUND(15)@1602162932497000/1602162932
                Marker INCL_END_BOUND(15)@1602162932497000/1602162932
                 */
            });
        }
    }

    @Test
    public void rrFake3Test() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck > 10 AND ck < 20");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck > 15", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(15)@1602163225503000/1602163225
                Marker EXCL_END_BOUND(20)@1602163225503000/1602163225
                 */
            });
        }
    }


    @Test
    public void rrFake4Test() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck > 10 AND ck < 20");
            cluster.get(2).flush(KEYSPACE);
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck > 15", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(15)@1602163277639000/1602163277
                Marker EXCL_END_BOUND(20)@1602163277639000/1602163277
                 */
            });
        }
    }

    @Test
    public void rrFake5Test() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck > 10 AND ck < 20");
            cluster.get(2).flush(KEYSPACE);
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(10)@1602163579254000/1602163579
                Marker EXCL_END_BOUND(20)@1602163579254000/1602163579
                */
            });
        }
    }

    @Test
    public void rrFake6Test() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5 AND ck = 10");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck > 5 and ck < 15", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Row[info=[ts=-9223372036854775808] del=deletedAt=1602163749938000, localDeletion=1602163749 ]: ck=10 |
                 */
            });
        }
    }



    private static void dump(ColumnFamilyStore cfs)
    {
        SSTableReader sstable = Iterables.getFirst(cfs.getLiveSSTables(), null);
        if (sstable == null) throw new AssertionError("should have one sstable");
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator uri = scanner.next())
                {
                    System.out.println("XXX "+uri.partitionKey());
                    System.out.println("partitionLevelDeletion: "+uri.partitionLevelDeletion());
                    while (uri.hasNext())
                    {
                        Unfiltered next = uri.next();
                        System.out.println(next.toString(cfs.metadata(), true));
                    }
                }

            }
        }
    }
}
