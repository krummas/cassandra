package org.apache.cassandra.concurrent;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.WrappedRunnable;

public class DebuggableThreadPoolExecutorTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerialization()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = System.nanoTime();
        for (int i = 0; i < 10; i++)
        {
            executor.execute(runnable);
        }
        assert q.size() > 0 : q.size();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assert delta >= 9 * 50 : delta;
    }

    @Test
    public void testRunnableFuturesWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        withTracing(() -> {
            try {

                Throwable throwable = assertThrows(ExecutionException.class, () -> executor.submit(failingTask()).get());
                Assert.assertEquals(DebuggingThrowsException.class, throwable.getCause().getClass());

                throwable = assertThrows(ExecutionException.class, () -> executor.submit(failingTask(), 42).get());
                Assert.assertEquals(DebuggingThrowsException.class, throwable.getCause().getClass());
            }
            finally
            {
                executor.shutdown();
            }
        });

    }

    private static void withTracing(Runnable fn)
    {
        TraceState state = Tracing.instance.get();
        try {
            Tracing.instance.set(new TraceStateImpl(InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.0.1")), UUID.randomUUID(), Tracing.TraceType.NONE));
            fn.run();
        }
        finally
        {
            Tracing.instance.set(state);
        }
    }

    private static String failingFunction()
    {
        throw new DebuggingThrowsException();
    }

    private static RunnableFuture<String> failingTask()
    {
        return ListenableFutureTask.create(DebuggableThreadPoolExecutorTest::failingFunction);
    }

    private static final class DebuggingThrowsException extends RuntimeException {

    }

    private static Throwable assertThrows(Class<? extends Throwable> klass, FailingRunnable fn)
    {
        Throwable throwed = null;
        try {
            fn.run();
        }
        catch (Throwable t)
        {
            throwed = t;
        }
        if (throwed == null)
            Assert.fail("Excepted to throw " + klass.getName() + " but did not throw a exception");
        if (!throwed.getClass().isAssignableFrom(klass))
            Assert.fail("Excepted to throw " + klass.getName() + " but threw " + throwed.getClass().getName() + "; " + throwed.getMessage());
        return throwed;
    }

    public interface FailingRunnable
    {
        void run() throws Throwable;
    }
}
