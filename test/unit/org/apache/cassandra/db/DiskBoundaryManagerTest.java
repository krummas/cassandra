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

package org.apache.cassandra.db;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiskBoundaryManagerTest extends CQLTester
{
    private DiskBoundaryManager dbm;
    private MockCFS mock;
    private Directories dirs;

    @Before
    public void setup()
    {
        BlacklistedDirectories.clearUnwritableUnsafe();
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddress());
        createTable("create table %s (id int primary key, x text)");
        dbm = getCurrentColumnFamilyStore().diskBoundaryManager;
        dirs = new Directories(getCurrentColumnFamilyStore().metadata, Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/1")),
                                                                                          new Directories.DataDirectory(new File("/tmp/2")),
                                                                                          new Directories.DataDirectory(new File("/tmp/3"))));
        mock = new MockCFS(getCurrentColumnFamilyStore(), dirs);
    }

    @Test
    public void getBoundariesTest()
    {
        DiskBoundaryManager.DiskBoundaries dbv = dbm.getDiskBoundaries(mock);
        assertEquals(3, dbv.positions.size());
        assertTrue(Arrays.equals(dbv.directories, dirs.getWriteableLocations()));
    }

    @Test
    public void blackListTest()
    {
        DiskBoundaryManager.DiskBoundaries dbv = dbm.getDiskBoundaries(mock);
        assertEquals(3, dbv.positions.size());
        assertTrue(Arrays.equals(dbv.directories, dirs.getWriteableLocations()));
        BlacklistedDirectories.maybeMarkUnwritable(new File("/tmp/3"));
        dbv = dbm.getDiskBoundaries(mock);
        assertEquals(2, dbv.positions.size());
        assertTrue(Arrays.equals(new Directories.DataDirectory[] {new Directories.DataDirectory(new File("/tmp/1")),
                                                                  new Directories.DataDirectory(new File("/tmp/2"))},
                                 dbv.directories));
    }

    @Test
    public void updateTokensTest() throws UnknownHostException
    {
        DiskBoundaryManager.DiskBoundaries dbv1 = dbm.getDiskBoundaries(mock);
        StorageService.instance.getTokenMetadata().updateNormalTokens(BootStrapper.getRandomTokens(StorageService.instance.getTokenMetadata(), 10), InetAddress.getByName("127.0.0.10"));
        DiskBoundaryManager.DiskBoundaries dbv2 = dbm.getDiskBoundaries(mock);
        assertFalse(dbv1.equals(dbv2));
    }

    @Test
    public void alterKeyspaceTest() throws Throwable
    {
        DiskBoundaryManager.DiskBoundaries dbv1 = dbm.getDiskBoundaries(mock);
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        DiskBoundaryManager.DiskBoundaries dbv2 = dbm.getDiskBoundaries(mock);
        // == on purpose - we just want to make sure that there is a new instance cached
        assertFalse(dbv1 == dbv2);
        DiskBoundaryManager.DiskBoundaries dbv3 = dbm.getDiskBoundaries(mock);
        assertTrue(dbv2 == dbv3);

    }

    // just to be able to override the data directories
    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), 0, cfs.metadata, dirs, false, false, true);
        }
    }
}
