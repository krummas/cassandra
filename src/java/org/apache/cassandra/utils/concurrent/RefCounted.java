/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;

/**
 * An object that needs ref counting does the following:
 *   - defines a Tidy object that will cleanup once it's gone,
 *     (this must retain no references to the object we're tracking (only its resources and how to clean up))
 *   - implements RefCounted
 *   - encapsulates a RefCounted.Impl, to which it proxies all calls to RefCounted behaviours
 *   - ensures no external access to the encapsulated Impl, and permits no references to it to leak
 *   - users must ensure no references to the sharedRef leak, or are retained outside of a method scope either.
 *     (to ensure the sharedRef is collected with the object, so that leaks may be detected and corrected)
 *
 * This class' functionality is achieved by what may look at first glance like a complex web of references,
 * but boils down to:
 *
 * Target --> Impl --> sharedRef --> [RefState] <--> RefCountedState --> Tidy
 *                                        ^                ^
 *                                        |                |
 * Ref -----------------------------------                 |
 *                                                         |
 * Global -------------------------------------------------
 *
 * So that, if Target is collected, Impl is collected and, hence, so is sharedRef.
 *
 * Once ref or sharedRef are collected, the paired RefState's release method is called, which if it had
 * not already been called will update RefCountedState and log an error.
 *
 * Once the RefCountedState has been completely released, the Tidy method is called and it removes the global reference
 * to itself so it may also be collected.
 */
public interface RefCounted
{
    static final Logger logger = LoggerFactory.getLogger(RefCounted.class);
    static final boolean DEBUG_ENABLED = System.getProperty("cassandra.debugrefcount", "false").equalsIgnoreCase("true");

    /**
     * @return the a new Ref() to the managed object, incrementing its refcount
     */
    public Ref ref();

    /**
     * @return the shared Ref that is created at instantiation of the RefCounted instance.
     * Once released, if no other ref()s are extant the object will be tidied; references to
     * this object should never be retained outside of a method's scope
     */
    public Ref sharedRef();

    public static interface Tidy
    {
        void tidy();
        String name();
    }

    // default implementation; can be hidden and proxied (like we do for SSTableReader)
    static final class Impl implements RefCounted
    {
        private final Ref sharedRef;
        private final RefCountedState state;

        public Impl(Tidy tidy)
        {
            this.state = new RefCountedState(tidy);
            sharedRef = new Ref(this.state, true);
            Manager.extant.put(this.state, Boolean.TRUE);
        }

        public Ref ref()
        {
            return state.ref() ? new Ref(state, false) : null;
        }

        public Ref sharedRef()
        {
            return sharedRef;
        }
    }

    // the object that manages the actual cleaning up; this does not reference the RefCounted.Impl
    // so that we can detect when references are lost to the resource itself, and still cleanup afterwards
    // the Tidy object MUST not contain any references to the object we are managing
    static final class RefCountedState
    {
        // we need to retain a reference to each of the PhantomReference instances
        // we are using to track individual ref()s
        private final ConcurrentLinkedQueue<RefState> refs = new ConcurrentLinkedQueue<>();
        // the number of live ref()s
        private final AtomicInteger counts = new AtomicInteger();
        // the object to call to cleanup when our refs are all finished with
        private final Tidy tidy;

        public RefCountedState(Tidy tidy)
        {
            this.tidy = tidy;
        }

        // increment ref count if not already tidied, and return success/failure
        boolean ref()
        {
            while (true)
            {
                int cur = counts.get();
                if (cur < 0)
                    return false;
                if (counts.compareAndSet(cur, cur + 1))
                    return true;
            }
        }

        // release a single reference, and cleanup if no more are extant
        private void release()
        {
            if (-1 == counts.decrementAndGet())
            {
                tidy.tidy();
                Manager.extant.remove(this);
            }
        }
    }

    // similar to RefCountedState, but tracks only the management of each unique ref() created to the managed object
    // ensures it is only released once, and that it is always released
    static final class RefState extends PhantomReference<Ref>
    {
        final Debug debug = DEBUG_ENABLED ? new Debug() : null;
        final boolean isSharedRef;
        final RefCountedState state;
        private volatile int released;

        private static final AtomicIntegerFieldUpdater<RefState> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(RefState.class, "released");

        public RefState(final RefCountedState state, Ref reference, ReferenceQueue<? super Ref> q, boolean isSharedRef)
        {
            super(reference, q);
            this.state = state;
            this.isSharedRef = isSharedRef;
            state.refs.add(this);
        }

        void release(boolean leak)
        {
            if (!releasedUpdater.compareAndSet(this, 0, 1))
            {
                if (!leak)
                {
                    String id = this.toString();
                    logger.error("BAD RELEASE: attempted to release a{} reference ({}) that has already been released", isSharedRef ? " shared" : "", id);
                    if (DEBUG_ENABLED)
                        debug.log(id);
                    throw new IllegalStateException("Attempted to release a reference that has already been released");
                }
                return;
            }
            state.release();
            if (leak)
            {
                String id = this.toString();
                if (isSharedRef)
                    logger.error("LEAK DETECTED: the shared reference ({}) to {} was not released before the object was garbage collected", id, state.tidy.name());
                else
                    logger.error("LEAK DETECTED: a reference ({}) to {} was not released before the reference was garbage collected", id, state.tidy.name());
                if (DEBUG_ENABLED)
                    debug.log(id);
            }
            else if (DEBUG_ENABLED)
            {
                debug.deallocate();
            }
            state.refs.remove(this);
        }
    }

    /**
     * A single managed reference to a RefCounted object
     */
    public static final class Ref
    {
        final RefState state;

        Ref(RefCountedState state, boolean isSharedRef)
        {
            this.state = new RefState(state, this, Manager.referenceQueue, isSharedRef);
        }

        public void release()
        {
            state.release(false);
        }

        public int globalCount()
        {
            return 1 + state.state.counts.get();
        }
    }

    /**
     * A collection of managed references to RefCounted objects, and the objects they are linked to
     */
    public static final class Refs<T extends RefCounted> extends AbstractCollection<T> implements AutoCloseable
    {
        private final Map<T, Ref> references;
        public Refs()
        {
            this.references = new HashMap<>();
        }
        public Refs(Map<T, Ref> references)
        {
            this.references = new HashMap<>(references);
        }
        public void release()
        {
            release(references.values());
        }

        private static void release(Iterable<Ref> refs)
        {
            for (Ref ref : refs)
                ref.release();
        }

        public Ref get(T referenced)
        {
            return references.get(referenced);
        }

        public void release(T referenced)
        {
            references.remove(referenced).release();
        }

        public boolean releaseIfHolds(T referenced)
        {
            Ref ref = references.remove(referenced);
            if (ref != null)
                ref.release();
            return ref != null;
        }

        public void release(Collection<T> release)
        {
            for (T t : release)
                release(t);
        }

        public boolean ref(T t)
        {
            Ref ref = t.ref();
            if (ref == null)
                return false;
            ref =references.put(t, ref);
            if (ref != null)
                ref.release(); // release dup
            return true;
        }

        public Iterator<T> iterator()
        {
            return references.keySet().iterator();
        }

        public int size()
        {
            return references.size();
        }

        public static <T extends RefCounted> Refs<T> merge(List<Refs<T>> references)
        {
            Map<T, Ref> builder = new HashMap<>();
            for (Refs<T> refs : references)
            {
                for (Map.Entry<T, Ref> e : refs.references.entrySet())
                {
                    Ref collision = builder.put(e.getKey(), e.getValue());
                    if (collision != null)
                        collision.release();
                }
            }
            return new Refs<>(ImmutableMap.copyOf(builder));
        }

        public static <T extends RefCounted> Refs<T> tryRef(Iterable<T> reference)
        {
            HashMap<T, Ref> refs = new HashMap<>();
            for (T rc : reference)
            {
                Ref ref = rc.ref();
                if (ref == null)
                {
                    release(refs.values());
                    return null;
                }
                refs.put(rc, ref);
            }
            return new Refs<T>(refs);
        }
        public static <T extends RefCounted> Refs<T> ref(Iterable<T> reference)
        {
            Refs<T> refs = tryRef(reference);
            if (refs != null)
                return refs;
            throw new IllegalStateException();
        }

        public void close()
        {
            release();
        }
    }

    static final class Debug
    {
        String allocateThread, deallocateThread;
        StackTraceElement[] allocateTrace, deallocateTrace;
        Debug()
        {
            Thread thread = Thread.currentThread();
            allocateThread = thread.toString();
            allocateTrace = thread.getStackTrace();
        }
        synchronized void deallocate()
        {
            Thread thread = Thread.currentThread();
            deallocateThread = thread.toString();
            deallocateTrace = thread.getStackTrace();
        }
        synchronized void log(String id)
        {
            logger.error("Allocate trace {}:\n{}", id, print(allocateThread, allocateTrace));
            if (deallocateThread != null)
                logger.error("Deallocate trace {}:\n{}", id, print(deallocateThread, deallocateTrace));
        }
        String print(String thread, StackTraceElement[] trace)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(thread.toString());
            sb.append("\n");
            for (StackTraceElement element : trace)
            {
                sb.append("\tat ");
                sb.append(element );
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    static final class Manager
    {
        private static final ConcurrentHashMap<RefCountedState, Boolean> extant = new ConcurrentHashMap<>();
        private static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
        private static final ExecutorService EXEC = Executors.newFixedThreadPool(1, new NamedThreadFactory("Reference-Reaper"));
        static
        {
            EXEC.execute(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        while (true)
                        {
                            Object obj = referenceQueue.remove();
                            if (obj instanceof RefState)
                            {
                                ((RefState) obj).release(true);
                            }
                        }
                    }
                    catch (InterruptedException e)
                    {
                    }
                    finally
                    {
                        EXEC.execute(this);
                    }
                }
            });
        }
    }

}
