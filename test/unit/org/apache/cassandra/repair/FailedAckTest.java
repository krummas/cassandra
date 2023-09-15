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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.utils.Closeable;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.utils.Property.qt;

public class FailedAckTest extends FuzzTestBase
{
    private enum RepairStage
    { PREPARE, VALIDATION, SYNC }
    
    @Test
    public void failedAck()
    {
        DatabaseDescriptor.getRepairRetrySpec().maxAttempts = new RetrySpec.MaxAttempt(Integer.MAX_VALUE);
        DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(1);
//        Gen<RepairJobStage> stageGen = Gens.enums().all(RepairJobStage.class);
        Gen<RepairStage> stageGen = Gens.constant(RepairStage.PREPARE);
        qt().withSeed(42220190747834842L).withPure(false).withExamples(10).check(rs -> {
            Cluster cluster = new Cluster(rs);
            enableMessageFaults(cluster);

            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node coordinator = coordinatorGen.next(rs);

                RepairCoordinator repair = coordinator.repair(KEYSPACE, irOption(rs, coordinator, KEYSPACE, ignore -> TABLES), false);
                repair.run();
                // make sure the failing node is not the coordinator, else messaging isn't used
                InetAddressAndPort failingAddress = rs.pick(repair.state.getNeighborsAndRanges().participants);
                Cluster.Node failingNode = cluster.nodes.get(failingAddress);
                RepairStage stage = stageGen.next(rs);
                switch (stage)
                {
                    case PREPARE:
                    {
                        ICompactionManager cm = failingNode.compactionManager();
                        Mockito.when(cm.getPendingTasks()).thenReturn(42);
                        closeables.add(() -> Mockito.when(cm.getPendingTasks()).thenReturn(0));
                    }
                    break;
                    case VALIDATION:
                    {
                        cluster.addListener(new MessageListener() {
                            @Override
                            public void preHandle(Cluster.Node node, Message<?> msg)
                            {
                                if (node != failingNode) return;
                                if (msg.verb() != Verb.VALIDATION_REQ) return;
                                ValidationRequest req = (ValidationRequest) msg.payload;
                                if (rs.nextBoolean())
                                {
                                    // fail ctx.repair().consistent.local.maybeSetRepairing(desc.parentSessionId);
                                    LocalSession session = node.activeRepairService.consistent.local.getSession(req.desc.parentSessionId);
                                    session.setState(ConsistentSession.State.FAILED);
                                }
                                else
                                {
                                    // fail previewKind(desc.parentSessionId);
                                    node.activeRepairService.removeParentRepairSession(req.desc.parentSessionId);
                                }
                                cluster.removeListener(this);
                            }
                        });
                    }
                    break;
                    case SYNC:
                    {
//                        closeables.add(failingNode.doValidation((cfs, validator) -> addMismatch(rs, cfs, validator)));
//                        List<InetAddressAndPort> addresses = ImmutableList.<InetAddressAndPort>builder().add(coordinator.addressAndPort).addAll(repair.state.getNeighborsAndRanges().participants).build();
//                        for (InetAddressAndPort address : addresses)
//                        {
//                            closeables.add(cluster.nodes.get(address).doSync(plan -> {
//                                long delayNanos = rs.nextLong(TimeUnit.SECONDS.toNanos(5), TimeUnit.MINUTES.toNanos(10));
//                                cluster.unorderedScheduled.schedule(() -> {
//                                    if (address == failingAddress || plan.getCoordinator().getPeers().contains(failingAddress))
//                                    {
//                                        SimulatedFault fault = new SimulatedFault("Sync failed");
//                                        for (StreamEventHandler handler : plan.handlers())
//                                            handler.onFailure(fault);
//                                    }
//                                    else
//                                    {
//                                        StreamState success = new StreamState(plan.planId(), plan.streamOperation(), Collections.emptySet());
//                                        for (StreamEventHandler handler : plan.handlers())
//                                            handler.onSuccess(success);
//                                    }
//                                }, delayNanos, TimeUnit.NANOSECONDS);
//                                return null;
//                            }));
//                        }
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown stage: " + stage);
                }

                cluster.processAll();
                Assertions.assertThat(repair.state.getResult().kind).describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example).isEqualTo(Completable.Result.Kind.FAILURE);
                switch (stage)
                {
                    case PREPARE:
                    {
                        Assertions.assertThat(repair.state.getResult().message)
                                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example)
                                  .contains("Got negative replies from endpoints [" + failingAddress + "]");
                    }
                    break;
                    case VALIDATION:
                    {
                        Assertions.assertThat(repair.state.getResult().message)
                                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example)
                                  .contains("Got VALIDATION_REQ failure from " + failingAddress + ": UNKNOWN");
                    }
                    break;
                    case SYNC:
                        AbstractStringAssert<?> a = Assertions.assertThat(repair.state.getResult().message).describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example);
                        // SymmetricRemoteSyncTask + AsymmetricRemoteSyncTask
                        // ... Sync failed between /[81fc:714:2c56:a2d3:faf3:eb7c:e4dd:cb9e]:54401 and /220.3.10.72:21402
                        // LocalSyncTask
                        // ... failed with error Sync failed
                        String failingMsg = repair.state.getResult().message;
                        if (failingMsg.contains("Sync failed between"))
                        {
                            a.contains("Sync failed between").contains(failingAddress.toString());
                        }
                        else
                        {
                            a.contains("failed with error Sync failed");
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown stage: " + stage);
                }
                closeables.forEach(Closeable::close);
                closeables.clear();
            }
        });
    }
}
