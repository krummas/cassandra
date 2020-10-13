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
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker INCL_START_BOUND(15)@1602498476594000/1602498476
                Marker INCL_END_BOUND(15)@1602498476594000/1602498476
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
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker INCL_START_BOUND(15)@1602498524469000/1602498524
                Marker INCL_END_BOUND(15)@1602498524469000/1602498524
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
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(15)@1602499763070000/1602499763
                Marker EXCL_END_BOUND(20)@1602499763070000/1602499763
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
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(15)@1602498932246000/1602498932
                Marker EXCL_END_BOUND(20)@1602498932246000/1602498932
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

                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Marker EXCL_START_BOUND(10)@1602498900980000/1602498900
                Marker EXCL_END_BOUND(20)@1602498900980000/1602498900
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
                /*
                XXX DecoratedKey(-7509452495886106294, 00000005)
                partitionLevelDeletion: deletedAt=-9223372036854775808, localDeletion=2147483647
                Row[info=[ts=-9223372036854775808] del=deletedAt=1602498612164000, localDeletion=1602498612 ]: ck=10 |
                 */
            });
        }
    }

    @Test
    public void rrFake7Test() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int, ck int, v int, PRIMARY KEY (id, ck))");
            cluster.get(2).executeInternal("DELETE FROM "+KEYSPACE+".tbl WHERE id = 5");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE id = 5 and ck = 10", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                dump(cfs);
            });
        }
    }

    @Test
    public void perfTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            StringBuilder table = new StringBuilder("CREATE TABLE ").append(KEYSPACE).append(".tbl (id int");
            StringBuilder cols = new StringBuilder();
            StringBuilder cks = new StringBuilder();
            StringBuilder pk = new StringBuilder(", PRIMARY KEY (id");
            StringBuilder markers = new StringBuilder("");
            StringBuilder where = new StringBuilder(" ");

            StringBuilder insert = new StringBuilder("INSERT INTO ").append(KEYSPACE).append(".tbl").append("(id");

            for (int i = 0; i < 100; i++)
            {
                cks.append(", ck").append(i).append(" int");
                cols.append(", col").append(i).append(" int");
                pk.append(", ck").append(i);
                if (i > 0)
                    markers.append(',');
                markers.append('?');
                insert.append(", ck").append(i);
                where.append(" AND ck").append(i).append("=?");
            }
            insert.append(", col%d) VALUES (1, ?, ").append(markers).append(')');

            table.append(cks).append(cols).append(pk).append(")) WITH COMPACTION = {'class': 'SizeTieredCompactionStrategy', 'enabled':'false'}");
            cluster.schemaChange(table.toString());

            for (int i = 0; i < 100; i++)
            {
                Integer [] vals = vals(101, 1000 - i);
                cluster.coordinator(1).execute(String.format(insert.toString(), i), ConsistencyLevel.ALL, (Object[]) vals);
                cluster.get(1).flush(KEYSPACE);
            }

            long [] times = new long[10];
            for (int j = 0; j < times.length; j++)
            {
                long start = System.nanoTime();
                for (int i = 0; i < 1000; i++)
                {
                    cluster.coordinator(1).execute("SELECT ck0, ck1 FROM " + KEYSPACE + ".tbl WHERE id=1 " + where, ConsistencyLevel.ALL, (Object[]) vals(100, 99));
                }
                times[j] = System.nanoTime() - start;
                System.out.println("xxx: "+(System.nanoTime() - start));
            }
            long sum = 0;
            for (long t : times)
                sum += t;
            System.out.println("xxxavg: "+(sum / times.length));
            //xxxavg: 3490811780
            //xxxavg: 3015965839 <- trunk
        }
    }

    private Integer[] vals(int count, int lastVal)
    {
        Integer [] v = new Integer[count];
        for (int i = 0; i < count; i++)
        {
            v[i] = i;
        }
        v[count-1]=lastVal;
        return v;
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
