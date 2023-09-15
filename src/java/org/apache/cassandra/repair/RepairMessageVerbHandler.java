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
package org.apache.cassandra.repair;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.SharedContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.Verb.VALIDATION_RSP;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    private static class Holder
    {
        private static final RepairMessageVerbHandler instance = new RepairMessageVerbHandler();
    }

    public static RepairMessageVerbHandler instance()
    {
        return Holder.instance;
    }

    private final SharedContext ctx;

    private RepairMessageVerbHandler()
    {
        this(SharedContext.Global.instance);
    }

    public RepairMessageVerbHandler(SharedContext ctx)
    {
        this.ctx = ctx;
    }

    private static final Logger logger = LoggerFactory.getLogger(RepairMessageVerbHandler.class);

    private boolean isIncremental(TimeUUID sessionID)
    {
        return ctx.repair().consistent.local.isSessionInProgress(sessionID);
    }

    private PreviewKind previewKind(TimeUUID sessionID) throws NoSuchRepairSessionException
    {
        ActiveRepairService.ParentRepairSession prs = ctx.repair().getParentRepairSession(sessionID);
        return prs != null ? prs.previewKind : PreviewKind.NONE;
    }

    public void doVerb(final Message<RepairMessage> message)
    {
        // TODO add cancel/interrupt message
        RepairJobDesc desc = message.payload.desc;
        try
        {
            switch (message.verb())
            {
                case PREPARE_MSG:
                {
                    PrepareMessage prepareMessage = (PrepareMessage) message.payload;
                    logger.debug("Preparing, {}", prepareMessage);
                    ParticipateState state = new ParticipateState(message.from(), prepareMessage);
                    if (!ctx.repair().register(state))
                    {
                        sendAck(message);
                        return;
                    }
                    if (!ctx.repair().verifyCompactionsPendingThreshold(prepareMessage.parentRepairSession, prepareMessage.previewKind))
                    {
                        // error is logged in verifyCompactionsPendingThreshold
                        state.phase.fail("Too many pending compactions");
                        sendFailureResponse(message);
                        return;
                    }

                    List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(prepareMessage.tableIds.size());
                    for (TableId tableId : prepareMessage.tableIds)
                    {
                        ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.getIfExists(tableId);
                        if (columnFamilyStore == null)
                        {
                            String reason = String.format("Table with id %s was dropped during prepare phase of repair",
                                                          tableId);
                            state.phase.fail(reason);
                            logErrorAndSendFailureResponse(reason, message);
                            return;
                        }
                        columnFamilyStores.add(columnFamilyStore);
                    }
                    ctx.repair().registerParentRepairSession(prepareMessage.parentRepairSession,
                                                                    message.from(),
                                                                    columnFamilyStores,
                                                                    prepareMessage.ranges,
                                                                    prepareMessage.isIncremental,
                                                                    prepareMessage.repairedAt,
                                                                    prepareMessage.isGlobal,
                                                                    prepareMessage.previewKind);
                    sendAck(message);
                }
                    break;

                case SNAPSHOT_MSG:
                {
                    logger.debug("Snapshotting {}", desc);
                    ParticipateState state = ctx.repair().participate(desc.parentSessionId);
                    if (state == null)
                    {
                        logErrorAndSendFailureResponse("Unknown repair " + desc.parentSessionId, message);
                        return;
                    }
                    final ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
                    if (cfs == null)
                    {
                        String reason = String.format("Table %s.%s was dropped during snapshot phase of repair %s",
                                                      desc.keyspace, desc.columnFamily, desc.parentSessionId);
                        state.phase.fail(reason);
                        logErrorAndSendFailureResponse(reason, message);
                        return;
                    }

                    ActiveRepairService.ParentRepairSession prs = ctx.repair().getParentRepairSession(desc.parentSessionId);
                    if (prs.setHasSnapshots())
                    {
                        TableRepairManager repairManager = cfs.getRepairManager();
                        if (prs.isGlobal)
                        {
                            repairManager.snapshot(desc.parentSessionId.toString(), prs.getRanges(), false);
                        }
                        else
                        {
                            repairManager.snapshot(desc.parentSessionId.toString(), desc.ranges, true);
                        }
                        logger.debug("Enqueuing response to snapshot request {} to {}", desc.sessionId, message.from());
                    }
                    sendAck(message);
                }
                    break;

                case VALIDATION_REQ:
                {
                    ValidationRequest validationRequest = (ValidationRequest) message.payload;
                    logger.debug("Validating {}", validationRequest);

                    ParticipateState participate = ctx.repair().participate(desc.parentSessionId);
                    if (participate == null)
                    {
                        logErrorAndSendFailureResponse("Unknown repair " + desc.parentSessionId, message);
                        return;
                    }

                    ValidationState vState = new ValidationState(desc, message.from());
                    if (!participate.register(vState))
                    {
                        sendAck(message);
                        logger.debug("Duplicate validation message found for parent={}, validation={}", participate.id, vState.id);
                        return;
                    }
                    try
                    {
                        // trigger read-only compaction
                        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(desc.keyspace, desc.columnFamily);
                        if (store == null)
                        {
                            String msg = String.format("Table %s.%s was dropped during validation phase of repair %s", desc.keyspace, desc.columnFamily, desc.parentSessionId);
                            vState.phase.fail(msg);
                            logErrorAndSendFailureResponse(msg, message);
                            ctx.messaging().send(Message.out(VALIDATION_RSP, new ValidationResponse(desc)), message.from());
                            return;
                        }

                        try
                        {
                            ctx.repair().consistent.local.maybeSetRepairing(desc.parentSessionId);
                        }
                        catch (Throwable t)
                        {
                            JVMStabilityInspector.inspectThrowable(t);
                            logErrorAndSendFailureResponse(t.toString(), message);
                            return;
                        }
                        PreviewKind previewKind;
                        try
                        {
                            previewKind = previewKind(desc.parentSessionId);
                        }
                        catch (NoSuchRepairSessionException e)
                        {
                            logger.warn("Parent repair session {} has been removed, failing repair", desc.parentSessionId);
                            vState.phase.fail(e);
                            sendFailureResponse(message);
                            return;
                        }
                        sendAck(message);

                        Validator validator = new Validator(ctx, vState, validationRequest.nowInSec,
                                                            isIncremental(desc.parentSessionId), previewKind);
                        ctx.validationManager().submitValidation(store, validator);
                    }
                    catch (Throwable t)
                    {
                        vState.phase.fail(t);
                        throw t;
                    }
                }
                    break;

                case SYNC_REQ:
                {
                    // forwarded sync request
                    SyncRequest request = (SyncRequest) message.payload;
                    logger.debug("Syncing {}", request);

                    ParticipateState participate = ctx.repair().participate(desc.parentSessionId);
                    if (participate == null)
                    {
                        logErrorAndSendFailureResponse("Unknown repair " + desc.parentSessionId, message);
                        return;
                    }
                    sendAck(message);
                    if (!participate.registerStreaming(request.deterministicId()))
                    {
                        logger.debug("Duplicate sync message found for parent={}, validation={}", participate.id, desc.determanisticId());
                        return;
                    }
                    StreamingRepairTask task = new StreamingRepairTask(ctx, desc,
                                                                       request.initiator,
                                                                       request.src,
                                                                       request.dst,
                                                                       request.ranges,
                                                                       isIncremental(desc.parentSessionId) ? desc.parentSessionId : null,
                                                                       request.previewKind,
                                                                       request.asymmetric);
                    task.run();
                }
                    break;

                case CLEANUP_MSG:
                {
                    logger.debug("cleaning up repair");
                    CleanupMessage cleanup = (CleanupMessage) message.payload;
                    ParticipateState state = ctx.repair().participate(cleanup.parentRepairSession);
                    if (state != null)
                        state.phase.success("Cleanup message recieved");
                    ctx.repair().removeParentRepairSession(cleanup.parentRepairSession);
                    sendAck(message);
                }
                    break;

                case PREPARE_CONSISTENT_REQ:
                    ctx.repair().consistent.local.handlePrepareMessage(message.from(), (PrepareConsistentRequest) message.payload);
                    break;

                case PREPARE_CONSISTENT_RSP:
                    ctx.repair().consistent.coordinated.handlePrepareResponse((PrepareConsistentResponse) message.payload);
                    break;

                case FINALIZE_PROPOSE_MSG:
                    ctx.repair().consistent.local.handleFinalizeProposeMessage(message.from(), (FinalizePropose) message.payload);
                    break;

                case FINALIZE_PROMISE_MSG:
                    ctx.repair().consistent.coordinated.handleFinalizePromiseMessage((FinalizePromise) message.payload);
                    break;

                case FINALIZE_COMMIT_MSG:
                    ctx.repair().consistent.local.handleFinalizeCommitMessage(message.from(), (FinalizeCommit) message.payload);
                    break;

                case FAILED_SESSION_MSG:
                    FailSession failure = (FailSession) message.payload;
                    ctx.repair().consistent.coordinated.handleFailSessionMessage(failure);
                    ctx.repair().consistent.local.handleFailSessionMessage(message.from(), failure);
                    break;

                case STATUS_REQ:
                    ctx.repair().consistent.local.handleStatusRequest(message.from(), (StatusRequest) message.payload);
                    break;

                case STATUS_RSP:
                    ctx.repair().consistent.local.handleStatusResponse(message.from(), (StatusResponse) message.payload);
                    break;

                default:
                    ctx.repair().handleMessage(message);
                    break;
            }
        }
        catch (Exception e)
        {
            logger.error("Got error, removing parent repair session");
            if (desc != null && desc.parentSessionId != null)
            {
                ParticipateState parcipate = ctx.repair().participate(desc.parentSessionId);
                if (parcipate != null)
                    parcipate.phase.fail(e);
                ctx.repair().removeParentRepairSession(desc.parentSessionId);
            }
            throw new RuntimeException(e);
        }
    }

    private void logErrorAndSendFailureResponse(String errorMessage, Message<?> respondTo)
    {
        logger.error(errorMessage);
        sendFailureResponse(respondTo);
    }

    private void sendFailureResponse(Message<?> respondTo)
    {
        Message<?> reply = respondTo.failureResponse(RequestFailureReason.UNKNOWN);
        ctx.messaging().send(reply, respondTo.from());
    }

    private void sendAck(Message<RepairMessage> message)
    {
        ctx.messaging().send(message.emptyResponse(), message.from());
    }
}
