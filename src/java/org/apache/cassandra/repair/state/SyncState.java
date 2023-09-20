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

import java.util.UUID;

import org.apache.cassandra.repair.RepairJobDesc;

public class SyncState extends AbstractState<SyncState.State, UUID>
{
    public enum State
    { ACCEPT, PLANNING, START }

    public final Phase phase = new Phase();
    public final RepairJobDesc desc;

    public SyncState(UUID id, RepairJobDesc desc)
    {
        super(id, State.class);
        this.desc = desc;
    }

    public final class Phase extends BaseSkipPhase
    {
        public void accept()
        {
            updateState(State.ACCEPT);
        }

        public void planning()
        {
            updateState(State.PLANNING);
        }

        public void start()
        {
            updateState(State.START);
        }
    }
}
