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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PendingRepairManagerTest extends AbstractPendingRepairTest
{
    /**
     * If a local session is ongoing, it should not be cleaned up
     */
    @Test
    public void needsCleanupInProgress()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertFalse(prm.needsCleanup(repairID));
    }

    /**
     * If a local session is finalized, it should be cleaned up
     */
    @Test
    public void needsCleanupFinalized()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertTrue(prm.needsCleanup(repairID));
    }

    /**
     * If a local session has failed, it should be cleaned up
     */
    @Test
    public void needsCleanupFailed()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertTrue(prm.needsCleanup(repairID));
    }

    @Test
    public void needsCleanupNoSession()
    {
        UUID fakeID = UUIDGen.getTimeUUID();
        PendingRepairManager prm = new PendingRepairManager(cfs, null);
        Assert.assertTrue(prm.needsCleanup(fakeID));
    }

    @Test
    public void estimateRemainingTasksInProgress()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));

        Assert.assertEquals(0, prm.getEstimatedRemainingTasks());
    }

    @Test
    public void estimateRemainingTasksNeedsCleanup()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(1, prm.getEstimatedRemainingTasks());
    }

    @Test
    public void getNextBackgroundTask()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);

        repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(2, prm.getSessions().size());
        AbstractCompactionTask compactionTask = prm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());
        PendingRepairManager.RepairFinishedCompactionTask cleanupTask = (PendingRepairManager.RepairFinishedCompactionTask) compactionTask;
        Assert.assertEquals(repairID, cleanupTask.getSessionID());
    }

    @Test
    public void getNextBackgroundTaskNoSessions()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);
        Assert.assertNull(prm.getNextBackgroundTask(FBUtilities.nowInSeconds()));
    }

    @Test
    public void maximalTaskNeedsCleanup()
    {
        PendingRepairManager prm = csm.getPendingRepairManagers().get(0);

        UUID repairID = registerSession(cfs);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        prm.addSSTable(sstable);
        Assert.assertNotNull(prm.get(repairID));
        Assert.assertNotNull(prm.get(repairID));
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertEquals(1, prm.getMaximalTasks(FBUtilities.nowInSeconds(), false).size());
    }
}
