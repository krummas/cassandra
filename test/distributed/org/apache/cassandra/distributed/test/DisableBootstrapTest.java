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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DisableBootstrapTest extends TestBaseImpl
{
    @Test
    public void testDisableBootstrap() throws IOException
    {
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(originalNodeCount)
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL))
                                           .start()))
        {
            IInstanceConfig nodeConfig = cluster.newInstanceConfig();
            nodeConfig.set("auto_bootstrap", true);
            nodeConfig.set("ring_changes_disabled", true);
            IInvokableInstance instance = cluster.bootstrap(nodeConfig);
            try
            {
                instance.startup();
                fail("Bootstrap should fail");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Ring changes are disabled"));
            }
        }
    }
}
