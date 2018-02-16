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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ImportTest extends CQLTester
{

    @Test
    public void basicImportTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();

        File backupdir = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, false, false);

        assertEquals(10, execute("select * from %s").size());
    }

    @Test
    @Deprecated
    public void refreshTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        getCurrentColumnFamilyStore().clearUnsafe();
        assertEquals(0, execute("select * from %s").size());
        getCurrentColumnFamilyStore().loadNewSSTables();
        assertEquals(10, execute("select * from %s").size());
    }

    @Test
    public void importResetLevelTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        for (SSTableReader sstable : sstables)
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 123);
        File backupdir = moveToBackupDir(sstables);
        assertEquals(0, execute("select * from %s").size());

        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, false, false);

        assertEquals(10, execute("select * from %s").size());
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : sstables)
            assertEquals(123, sstable.getSSTableLevel());

        getCurrentColumnFamilyStore().clearUnsafe();
        backupdir = moveToBackupDir(sstables);
        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), true, false, false, false);
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertEquals(0, sstable.getSSTableLevel());
    }


    @Test
    public void importClearRepairedTest() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        getCurrentColumnFamilyStore().clearUnsafe();
        for (SSTableReader sstable : sstables)
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, 111, null);

        File backupdir = moveToBackupDir(sstables);

        assertEquals(0, execute("select * from %s").size());

        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, false, false);

        assertEquals(10, execute("select * from %s").size());
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : sstables)
            assertTrue(sstable.isRepaired());

        getCurrentColumnFamilyStore().clearUnsafe();
        backupdir = moveToBackupDir(sstables);
        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, true, false, false);
        sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals(1, sstables.size());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertFalse(sstable.isRepaired());
    }

    private File moveToBackupDir(Set<SSTableReader> sstables) throws IOException
    {
        Path temp = Files.createTempDirectory("importtest");
        SSTableReader sst = sstables.iterator().next();
        String tabledir = sst.descriptor.directory.getName();
        String ksdir = sst.descriptor.directory.getParentFile().getName();
        Path backupdir = Files.createDirectories(Paths.get(temp.toString(), ksdir, tabledir));

        for (SSTableReader sstable : sstables)
        {
            for (File f : sstable.descriptor.directory.listFiles())
            {
                if (f.toString().contains(sstable.descriptor.baseFilename()))
                {
                    System.out.println("move " + f.toPath() + " to " + backupdir);
                    Files.move(f.toPath(), new File(backupdir.toFile(), f.getName()).toPath());
                }
            }
        }
        return backupdir.toFile();

    }

    @Test
    public void testBestDisk() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 1), InetAddressAndPort.getByName("127.0.0.1"));
        Directories dirs = new Directories(getCurrentColumnFamilyStore().metadata(), Lists.newArrayList(new Directories.DataDirectory(new File("/tmp/1")),
                                                                                                        new Directories.DataDirectory(new File("/tmp/2")),
                                                                                                        new Directories.DataDirectory(new File("/tmp/3"))));
        MockCFS mock = new MockCFS(getCurrentColumnFamilyStore(), dirs);

        int rows = 1000;
        Random rand = new Random(12345);
        for (int i = 0; i < rows; i++)
            execute("insert into %s (id, d) values (?, ?)", rand.nextInt(), i);
        UntypedResultSet res = execute("SELECT token(id) as t FROM %s");
        long disk1 = 0, disk2 = 0, disk3 = 0;
        for (UntypedResultSet.Row r : res)
        {
            long l = r.getLong("t");
            if (l <= -3074457345618258603L)
                disk1++;
            else if (l <= 3074457345618258602L)
                disk2++;
            else
                disk3++;
        }
        File expected;
        if (disk1 > disk2 && disk1 > disk3)
            expected = new File("/tmp/1");
        else if (disk2 > disk1 && disk2 > disk3)
            expected = new File("/tmp/2");
        else
            expected = new File("/tmp/3");

        getCurrentColumnFamilyStore().forceBlockingFlush();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        assertEquals(expected, ColumnFamilyStore.findBestDiskAndInvalidateCaches(mock, sstable.descriptor, "/tmp/"));
    }

    @Test
    public void testImportCorrupt() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        SSTableReader sstableToCorrupt = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i + 10, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        try (RandomAccessFile file = new RandomAccessFile(sstableToCorrupt.descriptor.filenameFor(Component.DIGEST), "rw"))
        {
            Long correctChecksum = Long.valueOf(file.readLine());
            VerifyTest.writeChecksum(++correctChecksum, sstableToCorrupt.descriptor.filenameFor(Component.DIGEST));
        }

        File backupdir = moveToBackupDir(sstables);
        try
        {
            getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, true, false);
            fail("loadNewSSTables should fail!");
        }
        catch (Throwable t)
        {
            for (File f : getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables().listFiles())
            {
                if (f.isFile())
                    fail("there should not be any sstables in the data directory after a failed import: " + f);
            }
        }
    }


    @Test(expected = RuntimeException.class)
    public void testImportOutOfRange() throws Throwable
    {
        createTable("create table %s (id int primary key, d int)");
        for (int i = 0; i < 1000; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();

        getCurrentColumnFamilyStore().clearUnsafe();

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalTokens(BootStrapper.getRandomTokens(tmd, 5), InetAddressAndPort.getByName("127.0.0.3"));


        File backupdir = moveToBackupDir(sstables);
        try
        {
            getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, true, true);
        }
        finally
        {
            tmd.clearUnsafe();
        }
    }


    @Test
    public void testImportInvalidateCache() throws Throwable
    {
        createTable("create table %s (id int primary key, d int) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }");
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        CacheService.instance.setRowCacheCapacityInMB(1);

        Set<RowCacheKey> keysToInvalidate = new HashSet<>();

        // populate the row cache with keys from the sstable we are about to remove
        for (int i = 0; i < 10; i++)
        {
            execute("SELECT * FROM %s WHERE id = ?", i);
        }
        Iterator<RowCacheKey> it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            keysToInvalidate.add(it.next());
        }
        SSTableReader sstableToImport = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        getCurrentColumnFamilyStore().clearUnsafe();


        for (int i = 10; i < 20; i++)
            execute("insert into %s (id, d) values (?, ?)", i, i);
        getCurrentColumnFamilyStore().forceBlockingFlush();

        Set<RowCacheKey> allKeys = new HashSet<>();

        // populate row cache with sstable we are keeping
        for (int i = 10; i < 20; i++)
        {
            execute("SELECT * FROM %s WHERE id = ?", i);
        }
        it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            allKeys.add(it.next());
        }

        File backupdir = moveToBackupDir(Collections.singleton(sstableToImport));
        getCurrentColumnFamilyStore().loadNewSSTables(backupdir.toString(), false, false, true, true);
        assertEquals(10, CacheService.instance.rowCache.size());
        it = CacheService.instance.rowCache.keyIterator();
        while (it.hasNext())
        {
            // make sure the keys from the sstable we are importing are invalidated and that the other one is still there
            RowCacheKey rck = it.next();
            assertTrue(allKeys.contains(rck));
            assertFalse(keysToInvalidate.contains(rck));
        }
    }

    private static class MockCFS extends ColumnFamilyStore
    {
        public MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), 0, cfs.metadata, dirs, false, false, true);
        }
    }
}
