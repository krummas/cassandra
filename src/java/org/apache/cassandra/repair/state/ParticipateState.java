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
package org.apache.cassandra.repair.state;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;

public class ParticipateState extends AbstractCompletable<TimeUUID>
{
    public enum RegisterStatus
    { ACCEPTED, EXISTS, STATUS_REJECTED, ALREADY_COMPLETED }
    public final InetAddressAndPort initiator;
    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;
    public final boolean incremental;
    public final long repairedAt;
    public final boolean global;
    public final PreviewKind previewKind;
    private volatile boolean accepted = false;

    public final Phase phase = new Phase();

    @Override
    public boolean isAccepted()
    {
        return accepted;
    }

    public static class Job extends AbstractState<Job.State, RepairJobDesc>
    {
        public enum State { ACCEPT, SNAPSHOT, VALIDATION, SYNC }

        // TODO (now, cleanup): only one validation should exist, this was just to make refactor easier for now
        private final ConcurrentMap<UUID, ValidationState> validations = new ConcurrentHashMap<>();
        private final ConcurrentMap<UUID, SyncState> syncs = new ConcurrentHashMap<>();

        public Job(RepairJobDesc desc)
        {
            super(desc, State.class);
        }

        @Override
        protected synchronized UpdateType maybeUpdateState(State state)
        {
            return super.maybeUpdateState(state);
        }

        public void snapshot()
        {
            updateState(State.SNAPSHOT);
        }

        public RegisterStatus register(ValidationState state)
        {
            return register(validations, State.VALIDATION, state);
        }

        @Nullable
        public ValidationState validation(UUID id)
        {
            return validations.get(id);
        }

        public RegisterStatus register(SyncState state)
        {
            return register(syncs, State.SYNC, state);
        }

        private <I, S extends AbstractState<?, I>> RegisterStatus register(ConcurrentMap<I, S> map, State state, S value)
        {
            UpdateType updateType = maybeUpdateState(state);
            switch (updateType)
            {
                case ALREADY_COMPLETED:
                    return RegisterStatus.ALREADY_COMPLETED;
                case LARGER_STATE_SEEN:
                    return RegisterStatus.STATUS_REJECTED;
                case ACCEPTED:
                case NO_CHANGE:
                    // allow
                    break;
                default:
                    throw new IllegalStateException("Unknown status: " + updateType);
            }
            S current = map.putIfAbsent(value.id, value);
            return current == null ? RegisterStatus.ACCEPTED : RegisterStatus.EXISTS;
        }

        @Nullable
        public SyncState sync(UUID id)
        {
            return syncs.get(id);
        }
    }

    public final ConcurrentMap<RepairJobDesc, Job> jobs = new ConcurrentHashMap<>();

    public ParticipateState(InetAddressAndPort initiator, PrepareMessage msg)
    {
        super(msg.parentRepairSession);
        this.initiator = initiator;
        this.tableIds = msg.tableIds;
        this.ranges = msg.ranges;
        this.incremental = msg.isIncremental;
        this.repairedAt = msg.repairedAt;
        this.global = msg.isGlobal;
        this.previewKind = msg.previewKind;
    }

    @Nullable
    public Job job(RepairJobDesc desc)
    {
        return jobs.get(desc);
    }

    public Job getOrCreateJob(RepairJobDesc desc)
    {
        return jobs.computeIfAbsent(desc, Job::new);
    }

    @Nullable
    public ValidationState validation(RepairJobDesc desc, UUID id)
    {
        Job job = job(desc);
        if (job == null)
            return null;
        return job.validation(id);
    }

    public RegisterStatus register(ValidationState state)
    {
        return getOrCreateJob(state.desc).register(state);
    }

    @Nullable
    public SyncState sync(RepairJobDesc desc, UUID id)
    {
        Job job = job(desc);
        if (job == null)
            return null;
        return job.sync(id);
    }

    public RegisterStatus register(SyncState state)
    {
        return getOrCreateJob(state.desc).register(state);
    }

    public Collection<ValidationState> validations()
    {
        return jobs.values().stream().flatMap(j -> j.validations.values().stream()).collect(Collectors.toList());
    }

    public Collection<UUID> validationIds()
    {
        return jobs.values().stream().flatMap(j -> j.validations.keySet().stream()).collect(Collectors.toList());
    }

    @Override
    public String toString()
    {
        Result result = getResult();
        return "ParticipateState{" +
               "initiator=" + initiator +
               ", status=" + (result == null ? "pending" : result.kind.name()) +
               ", jobs=" + jobs.values() +
               '}';
    }

    public class Phase extends BasePhase
    {
        public void accept()
        {
            accepted = true;
        }
    }
}
