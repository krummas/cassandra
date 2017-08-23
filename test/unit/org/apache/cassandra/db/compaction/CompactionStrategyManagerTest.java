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

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertTrue;

public class CompactionStrategyManagerTest extends CQLTester
{
    @Test
    public void first() throws Throwable
    {
        createTable("create table %s (id int primary key, something text)");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 500; i++)
        {
            execute("insert into %s (id, something) values ("+i+", 'abc')");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        Collection<Directories.DataDirectory> paths = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            paths.add(new Directories.DataDirectory(new File("/tmp/f" + i)));
        }

        ColumnFamilyStore cfsold = getCurrentColumnFamilyStore();
        ColumnFamilyStore cfs = new ColumnFamilyStore(cfsold.keyspace, cfsold.getTableName(), 0, cfsold.metadata, new Directories(cfsold.metadata, paths), true, false, true);

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = getCurrentColumnFamilyStore().getPartitioner();
        Collection<Token> tokens = new ArrayList<>(1500);
        for (int j = 1; j < 100; j++)
        {
            for (int i = 0; i < 1500; i++)
            {
                tokens.add(partitioner.getRandomToken());
            }
            tmd.updateNormalTokens(tokens, InetAddress.getByName("127.0.0." + j));
            tokens = new ArrayList<>(1500);
        }

        long start = System.currentTimeMillis();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            CompactionStrategyManager.getCompactionStrategyIndex(cfs, cfs.getDirectories(), sstable);

        long time = System.currentTimeMillis() - start;
        assertTrue(time < 10000);
    }
}
