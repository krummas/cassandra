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

package org.apache.cassandra.config;

import java.util.EnumMap;
import java.util.Map;

public class RepairRetrySpec extends RetrySpec
{
    public enum Verb
    {
        PREPARE,
        VALIDATION_REQ, VALIDATION_RSP,
        SYNC_REQ, SYNC_RSP,
        SNAPSHOT,
        CLEANUP
    }

    public Map<Verb, RetrySpec.Partial> verbs = new EnumMap<>(Verb.class);

    public RetrySpec get(Verb verb)
    {
        if (!verbs.containsKey(verb))
            return this;

        return verbs.get(verb).withDefaults(this);
    }

    public boolean isEnabled(Verb verb)
    {
        Partial partial = verbs.get(verb);
        if (partial == null || partial.maxAttempts == null)
            return isEnabled();
        return partial.isEnabled();
    }
}
