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
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.RepairResult;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.insert;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.options;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.repair;
import static org.junit.Assert.assertFalse;

public class IncRepairTruncationTest extends TestBaseImpl
{
    @Test
    public void testTruncateDuringIncRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newFixedThreadPool(3);
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("disable_incremental_repair", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer(BBHelper::install)
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            // everything repaired
            cluster.get(1).callOnInstance(repair(options(false, false)));

            /*
            start repair:
            node1 executes its anticompaction immediately
            node2 will sleep for 3 seconds (see the bytebuddy-modified getAcquisitionCallable below)
             */
            Future<RepairResult> repairResult = es.submit(() -> cluster.get(1).callOnInstance(repair(options(false, false))));
            /*
            make sure we are out-of-sync to make node2 stream data to node1:
             */
            cluster.get(2).executeInternal("insert into "+KEYSPACE+".tbl (id, t) values (5, 5)");
            /*
            at this point node1 will have anticompacted the sstables while node2 is still waiting for the 3s sleep
             */
            Future<?> f = es.submit( () -> cluster.coordinator(1).execute("TRUNCATE "+KEYSPACE+".tbl", ConsistencyLevel.ALL));
            /*
            now truncation waits for 5 seconds on node2, meaning node1 clears its sstables but node2 still has them

            during these 5 seconds the incremental will finish unless truncation aborts it, streaming data to node1

            once the 5 second truncation-sleep has finished node2 will get truncated while nothing will happen on node1 and we have a mismatch
             */

            f.get();
            repairResult.get();

            assertFalse(cluster.get(1).callOnInstance(repair(options(true, false))).wasInconsistent);

        }
        finally
        {
            es.shutdown();
        }
    }

    public static class BBHelper
    {
        @SuppressWarnings({ "unused", "UnstableApiUsage" })
        public static PendingAntiCompaction.AcquisitionCallable getAcquisitionCallable(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID prsId, int acquireRetrySeconds, int acquireSleepMillis, @SuperCall Callable<PendingAntiCompaction.AcquisitionCallable> zuper) throws Exception
        {
            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            return zuper.call();
        }

        @SuppressWarnings({ "unused", "UnstableApiUsage" })
        public static <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews, @SuperCall Callable<V> zuper) throws Exception
        {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            return zuper.call();
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num == 2)
            {
                new ByteBuddy().rebase(PendingAntiCompaction.class)
                               .method(named("getAcquisitionCallable"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(ColumnFamilyStore.class)
                               .method(named("runWithCompactionsDisabled").and(takesArguments(3)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}
