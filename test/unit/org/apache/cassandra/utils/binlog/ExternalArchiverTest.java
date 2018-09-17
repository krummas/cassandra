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

package org.apache.cassandra.utils.binlog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Sets;
import org.junit.Test;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.binlog.ExternalArchiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalArchiverTest
{
    @Test
    public void testArchiver() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String dir = s.right;
        Path logdirectory = Files.createTempDirectory("logdirectory");
        File logfileToArchive = Files.createTempFile(logdirectory, "logfile", "xyz").toFile();
        Files.write(logfileToArchive.toPath(), "content".getBytes());

        ExternalArchiver ea = new ExternalArchiver(script+" %path", null);
        ea.onReleased(1, logfileToArchive);
        while (logfileToArchive.exists())
        {
            Thread.sleep(100);
        }

        File movedFile = new File(dir, logfileToArchive.getName());
        assertTrue(movedFile.exists());
        movedFile.deleteOnExit();
        ea.stop();
        assertEquals(0, logdirectory.toFile().listFiles().length);
    }

    @Test
    public void testArchiveExisting() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String moveDir = s.right;
        List<File> existingFiles = new ArrayList<>();
        Path dir = Files.createTempDirectory("archive");
        for (int i = 0; i < 10; i++)
        {
            File logfileToArchive = Files.createTempFile(dir, "logfile", SingleChronicleQueue.SUFFIX).toFile();
            logfileToArchive.deleteOnExit();
            Files.write(logfileToArchive.toPath(), ("content"+i).getBytes());
            existingFiles.add(logfileToArchive);
        }

        ExternalArchiver ea = new ExternalArchiver(script + " %path", dir);
        boolean allGone = false;
        while (!allGone)
        {
            allGone = true;
            for (File f : existingFiles)
            {
                if (f.exists())
                {
                    allGone = false;
                    Thread.sleep(100);
                    break;
                }
                File movedFile = new File(moveDir, f.getName());
                assertTrue(movedFile.exists());
                movedFile.deleteOnExit();
            }
        }
        ea.stop();
        assertEquals(0, dir.toFile().listFiles().length);
    }

    @Test
    public void testArchiveOnShutdown() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String moveDir = s.right;
        Path dir = Files.createTempDirectory("archive");
        ExternalArchiver ea = new ExternalArchiver(script + " %path", dir);
        List<File> existingFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            File logfileToArchive = Files.createTempFile(dir, "logfile", SingleChronicleQueue.SUFFIX).toFile();
            logfileToArchive.deleteOnExit();
            Files.write(logfileToArchive.toPath(), ("content"+i).getBytes());
            existingFiles.add(logfileToArchive);
        }
        // ea.stop will archive all .cq4 files in the directory
        ea.stop();
        for (File f : existingFiles)
        {
            assertFalse(f.exists());
            File movedFile = new File(moveDir, f.getName());
            assertTrue(movedFile.exists());
            movedFile.deleteOnExit();
        }
    }

    @Test
    public void testRetries() throws IOException, InterruptedException
    {
        Pair<String, String> s = createFailingScript();
        String script = s.left;
        String moveDir = s.right;
        Path logdirectory = Files.createTempDirectory("logdirectory");
        File logfileToArchive = Files.createTempFile(logdirectory, "logfile", "xyz").toFile();
        Files.write(logfileToArchive.toPath(), "content".getBytes());
        ExternalArchiver ea = new ExternalArchiver(script + " %path", null, 1000);
        ea.onReleased(0, logfileToArchive);
        Thread.sleep(500);
        assertTrue(logfileToArchive.exists());
        Thread.sleep(2000);
        assertFalse(logfileToArchive.exists());
        File movedFile = new File(moveDir, logfileToArchive.getName());
        assertTrue(movedFile.exists());
    }

    private Pair<String, String> createScript() throws IOException
    {
        File f = Files.createTempFile("script", "", PosixFilePermissions.asFileAttribute(Sets.newHashSet(PosixFilePermission.OWNER_WRITE,
                                                                                                         PosixFilePermission.OWNER_READ,
                                                                                                         PosixFilePermission.OWNER_EXECUTE))).toFile();
        f.deleteOnExit();
        File dir = Files.createTempDirectory("archive").toFile();
        dir.deleteOnExit();
        String script = "#!/bin/sh\nmv $1 "+dir.getAbsolutePath();
        Files.write(f.toPath(), script.getBytes());
        return Pair.create(f.getAbsolutePath(), dir.getAbsolutePath());
    }

    private Pair<String, String> createFailingScript() throws IOException
    {
        File f = Files.createTempFile("script", "", PosixFilePermissions.asFileAttribute(Sets.newHashSet(PosixFilePermission.OWNER_WRITE,
                                                                                                         PosixFilePermission.OWNER_READ,
                                                                                                         PosixFilePermission.OWNER_EXECUTE))).toFile();
        f.deleteOnExit();
        File dir = Files.createTempDirectory("archive").toFile();
        dir.deleteOnExit();
        // first time this script executes it will find that dir is empty, copy the file in to dir and exit with error status code to trigger a retry
        // second time it will find that dir is not empty and just delete the old log file
        String script = "#!/bin/sh%n"+
                        "DIR=%s%n" +
                        "if [ $(ls -A $DIR) ]; then%n" +
                        "   rm $1%n"+
                        "else%n"+
                        "   cp $1 $DIR%n"+
                        "   exit 1%n"+
                        "fi%n";

        Files.write(f.toPath(), String.format(script, dir.getAbsolutePath()).getBytes());
        return Pair.create(f.getAbsolutePath(), dir.getAbsolutePath());
    }
}
