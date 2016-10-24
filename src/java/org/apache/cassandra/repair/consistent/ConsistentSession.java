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
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * State common to local and coordinator sessions
 */
public abstract class ConsistentSession
{
    public enum State
    {
        PREPARING(0),
        PREPARED(1),
        REPAIRING(2),
        FINALIZING(3),
        FINISHED(4),
        FAILED(5);

        State(int expectedOrdinal)
        {
            assert ordinal() == expectedOrdinal;
        }

        private static final Map<State, Set<State>> transitions = new EnumMap<State, Set<State>>(State.class) {{
            put(PREPARING, ImmutableSet.of(PREPARED, FAILED));
            put(PREPARED, ImmutableSet.of(REPAIRING, FAILED));
            put(REPAIRING, ImmutableSet.of(FINALIZING, FAILED));
            put(FINALIZING, ImmutableSet.of(FINISHED, FAILED));
            put(FINISHED, ImmutableSet.of());
            put(FAILED, ImmutableSet.of());
        }};

        public boolean canTransitionTo(State state)
        {
            return transitions.get(this).contains(state);
        }

        public static State valueOf(int ordinal)
        {
            return values()[ordinal];
        }
    }

    private volatile State state;
    public final UUID sessionID;
    public final InetAddress coordinator;
    public final ImmutableSet<UUID> cfIds;
    public final long repairedAt;
    public final ImmutableSet<Range<Token>> ranges;
    public final ImmutableSet<InetAddress> participants;

    ConsistentSession(AbstractBuilder builder)
    {
        builder.validate();
        this.state = builder.state;
        this.sessionID = builder.sessionID;
        this.coordinator = builder.coordinator;
        this.cfIds = ImmutableSet.copyOf(builder.cfIds);
        this.repairedAt = builder.repairedAt;
        this.ranges = ImmutableSet.copyOf(builder.ranges);
        this.participants = ImmutableSet.copyOf(builder.participants);
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsistentSession that = (ConsistentSession) o;

        if (repairedAt != that.repairedAt) return false;
        if (state != that.state) return false;
        if (!sessionID.equals(that.sessionID)) return false;
        if (!coordinator.equals(that.coordinator)) return false;
        if (!cfIds.equals(that.cfIds)) return false;
        if (!ranges.equals(that.ranges)) return false;
        return participants.equals(that.participants);
    }

    public int hashCode()
    {
        int result = state.hashCode();
        result = 31 * result + sessionID.hashCode();
        result = 31 * result + coordinator.hashCode();
        result = 31 * result + cfIds.hashCode();
        result = 31 * result + (int) (repairedAt ^ (repairedAt >>> 32));
        result = 31 * result + ranges.hashCode();
        result = 31 * result + participants.hashCode();
        return result;
    }

    public String toString()
    {
        return "ConsistentSession{" +
               "state=" + state +
               ", sessionID=" + sessionID +
               ", coordinator=" + coordinator +
               ", cfIds=" + cfIds +
               ", repairedAt=" + repairedAt +
               ", ranges=" + ranges +
               ", participants=" + participants +
               '}';
    }

    public abstract static class AbstractBuilder
    {
        private State state;
        private UUID sessionID;
        private InetAddress coordinator;
        private Set<UUID> cfIds;
        private long repairedAt;
        private Collection<Range<Token>> ranges;
        private Set<InetAddress> participants;

        public void withState(State state)
        {
            this.state = state;
        }

        public void withSessionID(UUID sessionID)
        {
            this.sessionID = sessionID;
        }

        public void withCoordinator(InetAddress coordinator)
        {
            this.coordinator = coordinator;
        }

        public void withCfIds(Set<UUID> cfIds)
        {
            this.cfIds = cfIds;
        }

        public void withRepairedAt(long repairedAt)
        {
            this.repairedAt = repairedAt;
        }

        public void withRanges(Collection<Range<Token>> ranges)
        {
            this.ranges = ranges;
        }

        public void withParticipants(Set<InetAddress> peers)
        {
            this.participants = peers;
        }

        void validate()
        {
            Preconditions.checkArgument(state != null);
            Preconditions.checkArgument(sessionID != null);
            Preconditions.checkArgument(coordinator != null);
            Preconditions.checkArgument(cfIds != null);
            Preconditions.checkArgument(!cfIds.isEmpty());
            Preconditions.checkArgument(repairedAt > 0);
            Preconditions.checkArgument(ranges != null);
            Preconditions.checkArgument(!ranges.isEmpty());
            Preconditions.checkArgument(participants != null);
            Preconditions.checkArgument(!participants.isEmpty());
            Preconditions.checkArgument(participants.contains(coordinator));
        }
    }


}
