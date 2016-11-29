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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;

public class LocalSessionTest extends ConsistentSessionTest
{

    static LocalSession.Builder createBuilder()
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(PREPARING);
        builder.withSessionID(UUIDGen.getTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withCfIds(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        int now = FBUtilities.nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return builder;
    }

    static LocalSession createSession()
    {
        return createBuilder().build();
    }

    private static void assertValidationFailure(Consumer<LocalSession.Builder> consumer)
    {
        try
        {
            LocalSession.Builder builder = createBuilder();
            consumer.accept(builder);
            builder.build();
            Assert.fail("Expected assertion error");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private static void assertMessageSent(InstrumentedLocalSessions coordinator, InetAddress participant, RepairMessage expected)
    {
        Assert.assertTrue(coordinator.sentMessages.containsKey(participant));
        Assert.assertEquals(1, coordinator.sentMessages.get(participant).size());
        Assert.assertEquals(expected, coordinator.sentMessages.get(participant).get(0));
    }

    static class InstrumentedLocalSessions extends LocalSessions
    {
        Map<InetAddress, List<RepairMessage>> sentMessages = new HashMap<>();
        protected void sendMessage(InetAddress destination, RepairMessage message)
        {
            if (!sentMessages.containsKey(destination))
            {
                sentMessages.put(destination, new ArrayList<>());
            }
            sentMessages.get(destination).add(message);
        }

        SettableFuture<Object> pendingAntiCompactionFuture = null;
        boolean submitPendingAntiCompactionCalled = false;
        ListenableFuture submitPendingAntiCompaction(LocalSession session, ExecutorService executor)
        {
            submitPendingAntiCompactionCalled = true;
            if (pendingAntiCompactionFuture != null)
            {
                return pendingAntiCompactionFuture;
            }
            else
            {
                return super.submitPendingAntiCompaction(session, executor);
            }
        }

        boolean failSessionCalled = false;
        public void failSession(UUID sessionID, boolean sendMessage)
        {
            failSessionCalled = true;
            super.failSession(sessionID, sendMessage);
        }
    }

    private static CFMetaData cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CFMetaData.compile("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "localsessiontest");
        SchemaLoader.createKeyspace("localsessiontest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.cfId);
    }

    @Before
    public void setup()
    {
        // clear out any data from previous test runs
        ColumnFamilyStore repairCfs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.REPAIRS);
        repairCfs.truncateBlocking();
    }

    private static UUID registerSession()
    {
        return registerSession(cfs);
    }

    @Test
    public void validation()
    {
        assertValidationFailure(b -> b.withState(null));
        assertValidationFailure(b -> b.withSessionID(null));
        assertValidationFailure(b -> b.withCoordinator(null));
        assertValidationFailure(b -> b.withCfIds(null));
        assertValidationFailure(b -> b.withCfIds(new HashSet<>()));
        assertValidationFailure(b -> b.withRepairedAt(0));
        assertValidationFailure(b -> b.withRepairedAt(-1));
        assertValidationFailure(b -> b.withRanges(null));
        assertValidationFailure(b -> b.withRanges(new HashSet<>()));
        assertValidationFailure(b -> b.withParticipants(null));
        assertValidationFailure(b -> b.withParticipants(new HashSet<>()));
        assertValidationFailure(b -> b.withStartedAt(0));
        assertValidationFailure(b -> b.withLastUpdate(0));
    }

    /**
     * Test that sessions are loaded and saved properly
     */
    @Test
    public void persistence()
    {
        LocalSessions sessions = new LocalSessions();
        LocalSession expected = createSession();
        sessions.save(expected);
        LocalSession actual = sessions.loadUnsafe(expected.sessionID);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void prepareSuccessCase()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        Assert.assertFalse(sessions.submitPendingAntiCompactionCalled);
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertTrue(sessions.submitPendingAntiCompactionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.pendingAntiCompactionFuture.set(new Object());
        session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // ...and we should have sent a success message back to the coordinator
        assertMessageSent(sessions, COORDINATOR, new PrepareConsistentResponse(sessionID, FBUtilities.getBroadcastAddress(), true));
    }

    /**
     * If anti compactionn fails, we should fail the session locally,
     * and send a failure message back to the coordinator
     */
    @Test
    public void prepareAntiCompactFailure()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        Assert.assertFalse(sessions.submitPendingAntiCompactionCalled);
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertTrue(sessions.submitPendingAntiCompactionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.pendingAntiCompactionFuture.setException(new RuntimeException());
        session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // ...and we should have sent a success message back to the coordinator
        assertMessageSent(sessions, COORDINATOR, new FailSession(sessionID));

    }

    /**
     * If a ParentRepairSession wasn't previously created, we shouldn't
     * create a session locally, but we should send a failure message to
     * the coordinator.
     */
    @Test
    public void prepareWithNonExistantParentSession()
    {
        UUID sessionID = UUIDGen.getTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertNull(sessions.getSession(sessionID));
        assertMessageSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    @Test
    public void maybeSetRepairing()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        sessions.sentMessages.clear();
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * Multiple calls to maybeSetRepairing shouldn't cause any problems
     */
    @Test
    public void maybeSetRepairingDuplicates()
    {

        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        // initial set
        sessions.sentMessages.clear();
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // repeated call 1
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // repeated call 2
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * We shouldn't fail if we don't have a session for the given session id
     */
    @Test
    public void maybeSetRepairingNonExistantSession()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        UUID fakeID = UUIDGen.getTimeUUID();
        sessions.maybeSetRepairing(fakeID);
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * In the success case, session state should be set to FINALIZE_PROMISED and
     * persisted, and a FinalizePromise message should be sent back to the coordinator
     */
    @Test
    public void finalizeProposeSuccessCase()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());
        sessions.maybeSetRepairing(sessionID);

        //
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());

        // should send a promised message to coordinator and set session state accordingly
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(new FinalizePropose(sessionID), COORDINATOR);
        Assert.assertEquals(FINALIZE_PROMISED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        assertMessageSent(sessions, COORDINATOR, new FinalizePromise(sessionID, FBUtilities.getBroadcastAddress(), true));
    }

    /**
     * Trying to propose finalization when the session isn't in the repaired
     * state should fail the session and send a failure message to the proposer
     */
    @Test
    public void finalizeProposeInvalidStateFailure()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        //
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        // should fail the session and send a failure message to the coordinator
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(new FinalizePropose(sessionID), COORDINATOR);
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        assertMessageSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    @Test
    public void finalizeProposeNonExistantSessionFailure()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        UUID fakeID = UUIDGen.getTimeUUID();
        sessions.handleFinalizeProposeMessage(new FinalizePropose(fakeID), COORDINATOR);
        Assert.assertNull(sessions.getSession(fakeID));
        assertMessageSent(sessions, COORDINATOR, new FailSession(fakeID));
    }

    /**
     * Session state should be set to finalized, sstables should be promoted
     * to repaired. No messages should be sent to the coordinator
     */
    @Test
    public void finalizeCommitSuccessCase()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to finalized promised
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(new FinalizePropose(sessionID), COORDINATOR);

        sessions.sentMessages.clear();
        LocalSession session = sessions.getSession(sessionID);
        sessions.handleFinalizeCommitMessage(new FinalizeCommit(sessionID));

        Assert.assertEquals(FINALIZED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    @Test
    public void finalizeCommitNonExistantSession()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        UUID fakeID = UUIDGen.getTimeUUID();
        sessions.handleFinalizeCommitMessage(new FinalizeCommit(fakeID));
        Assert.assertNull(sessions.getSession(fakeID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    @Test
    public void failSession()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(PREPARED, session.getState());
        sessions.sentMessages.clear();

        // fail session
        sessions.failSession(sessionID);
        Assert.assertEquals(FAILED, session.getState());
        assertMessageSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    /**
     * Session should be failed, but no messages should be sent
     */
    @Test
    public void handleFailMessage()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertEquals(PREPARED, session.getState());
        sessions.sentMessages.clear();

        sessions.handleFailSessionMessage(new FailSession(sessionID));
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * Check all states (except failed)
     */
    @Test
    public void isSessionInProgress()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.pendingAntiCompactionFuture = SettableFuture.create();  // prevent moving to prepared
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(PREPARED);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(REPAIRING);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(FINALIZE_PROMISED);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(FINALIZED);
        Assert.assertFalse(sessions.isSessionInProgress(sessionID));
    }

    @Test
    public void isSessionInProgressFailed()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        Assert.assertTrue(sessions.isSessionInProgress(sessionID));
        sessions.failSession(sessionID);
        Assert.assertFalse(sessions.isSessionInProgress(sessionID));
    }

    @Test
    public void isSessionInProgressNonExistantSession()
    {
        UUID fakeID = UUIDGen.getTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        Assert.assertFalse(sessions.isSessionInProgress(fakeID));
    }

    @Test
    public void finalRepairedAtFinalized()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(new FinalizePropose(sessionID), COORDINATOR);
        sessions.handleFinalizeCommitMessage(new FinalizeCommit(sessionID));

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertTrue(session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE);
        Assert.assertEquals(session.repairedAt, sessions.getFinalSessionRepairedAt(sessionID));
    }

    @Test
    public void finalRepairedAtFailed()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());
        sessions.failSession(sessionID);

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertTrue(session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE);
        long repairedAt = sessions.getFinalSessionRepairedAt(sessionID);
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, repairedAt);
    }

    @Test
    public void finalRepairedAtNoSession()
    {
        UUID fakeID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        long repairedAt = sessions.getFinalSessionRepairedAt(fakeID);
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, repairedAt);
    }

    @Test(expected = IllegalStateException.class)
    public void finalRepairedAtInProgress()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        sessions.getFinalSessionRepairedAt(sessionID);
    }

    /**
     * Startup happy path
     */
    @Test
    public void startup() throws Exception
    {
        InstrumentedLocalSessions initialSessions = new InstrumentedLocalSessions();
        initialSessions.start();
        Assert.assertEquals(0, initialSessions.getNumSessions());
        UUID id1 = registerSession();
        UUID id2 = registerSession();

        initialSessions.pendingAntiCompactionFuture = SettableFuture.create();
        initialSessions.handlePrepareMessage(new PrepareConsistentRequest(id1, COORDINATOR, PARTICIPANTS));
        initialSessions.handlePrepareMessage(new PrepareConsistentRequest(id2, COORDINATOR, PARTICIPANTS));
        initialSessions.pendingAntiCompactionFuture.set(new Object());
        Assert.assertEquals(2, initialSessions.getNumSessions());
        LocalSession session1 = initialSessions.getSession(id1);
        LocalSession session2 = initialSessions.getSession(id2);


        // subsequent startups should load persisted sessions
        InstrumentedLocalSessions nextSessions = new InstrumentedLocalSessions();
        Assert.assertEquals(0, nextSessions.getNumSessions());
        nextSessions.start();
        Assert.assertEquals(2, nextSessions.getNumSessions());

        Assert.assertEquals(session1, nextSessions.getSession(id1));
        Assert.assertEquals(session2, nextSessions.getSession(id2));
    }

    /**
     * If LocalSessions.start is called more than
     * once, an exception should be thrown
     */
    @Test (expected = IllegalArgumentException.class)
    public void multipleStartupFailure() throws Exception
    {
        InstrumentedLocalSessions initialSessions = new InstrumentedLocalSessions();
        initialSessions.start();
        initialSessions.start();
    }

    private static LocalSession sessionWithTime(int started, int updated)
    {
        LocalSession.Builder builder = createBuilder();
        builder.withStartedAt(started);
        builder.withLastUpdate(updated);
        return builder.build();
    }

    /**
     * Sessions that shouldn't be failed or deleted are left alone
     */
    @Test
    public void cleanupNoOp() throws Exception
    {
        LocalSessions sessions = new LocalSessions();
        sessions.start();

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT + 60;
        LocalSession session = sessionWithTime(time - 1, time);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertNotNull(sessions.getSession(session.sessionID));
    }

    /**
     * Sessions past the auto fail cutoff should be failed
     */
    @Test
    public void cleanupFail() throws Exception
    {
        LocalSessions sessions = new LocalSessions();
        sessions.start();

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession session = sessionWithTime(time - 1, time);
        session.setState(REPAIRING);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertNotNull(sessions.getSession(session.sessionID));
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(session.sessionID));
    }

    /**
     * Sessions past the auto delete cutoff should be deleted
     */
    @Test
    public void cleanupDelete() throws Exception
    {
        LocalSessions sessions = new LocalSessions();
        sessions.start();

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession failed = sessionWithTime(time - 1, time);
        failed.setState(FAILED);

        LocalSession finalized = sessionWithTime(time - 1, time);
        finalized.setState(FINALIZED);

        sessions.putSessionUnsafe(failed);
        sessions.putSessionUnsafe(finalized);
        Assert.assertNotNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        sessions.cleanup();

        Assert.assertNull(sessions.getSession(failed.sessionID));
        Assert.assertNull(sessions.getSession(finalized.sessionID));

        Assert.assertNull(sessions.loadUnsafe(failed.sessionID));
        Assert.assertNull(sessions.loadUnsafe(finalized.sessionID));
    }
}

