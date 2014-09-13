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
import java.lang.ref.SoftReference;
import java.util.AbstractCollection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;

public class AbstractRefCounted
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRefCounted.class);
    private final Ref sharedRef;
    private final State state;

    protected AbstractRefCounted(Tidy state)
    {
        this.state = new State(state, this, referenceQueue);
        sharedRef = new Ref(this.state);
        softReferences.put(this.state, Boolean.TRUE);
    }

    public Ref ref()
    {
        return state.ref() ? new Ref(state) : null;
    }

    public Ref sharedRef()
    {
        return sharedRef;
    }

    public static interface Tidy
    {
        void tidy();
        String name();
    }

    private static final class State extends SoftReference<AbstractRefCounted>
    {
        private final ConcurrentLinkedQueue<Link> refs = new ConcurrentLinkedQueue<>();
        private final AtomicInteger counts = new AtomicInteger();
        final Tidy tidy;
        public State(Tidy tidy, AbstractRefCounted referent, ReferenceQueue<? super AbstractRefCounted> q)
        {
            super(referent, q);
            this.tidy = tidy;
        }
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
        private void release()
        {
            if (-1 == counts.decrementAndGet())
                tidy.tidy();
        }
        private void tidyIfNotAlready()
        {
            if (counts.getAndSet(-1) >= 0)
            {
                logger.error("LEAK DETECTED: a reference to {} was not released before the object itself was GC'd", tidy.name());
                tidy.tidy();
            }
        }
    }

    private static final class Link extends PhantomReference<Ref>
    {
        final State state;
        private volatile int released;

        private static final AtomicIntegerFieldUpdater<Link> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(Link.class, "released");
        public Link(final State state, Ref reference, ReferenceQueue<? super Ref> q)
        {
            super(reference, q);
            this.state = state;
            state.refs.add(this);
        }
        void release(boolean leak)
        {
            if (!releasedUpdater.compareAndSet(this, 0, 1))
            {
                if (!leak)
                    throw new IllegalStateException("Attempted to release a reference that has already been released");
                return;
            }
            state.release();
            if (leak)
                logger.error("LEAK DETECTED: a reference to {} was not released before being GC'd", state.tidy.name());
            state.refs.remove(this);
        }
    }

    public static final class Ref
    {
        final Link link;

        public Ref(State state)
        {
            link = new Link(state, this, referenceQueue);
        }

        public void release()
        {
            link.release(false);
        }
    }

    public static final class Refs<T extends AbstractRefCounted> extends AbstractCollection<T>
    {
        private final Map<T, Ref> references;
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

        protected static <T extends AbstractRefCounted> Map<T, Ref> ref(Iterable<T> refCounted)
        {
            HashMap<T, Ref> builder = new HashMap<>();
            for (T rc : refCounted)
            {
                Ref ref = rc.ref();
                if (ref == null)
                {
                    release(builder.values());
                    return null;
                }
                builder.put(rc, ref);
            }
            return builder;
        }

        public Iterator<T> iterator()
        {
            return references.keySet().iterator();
        }

        public int size()
        {
            return references.size();
        }

        public static <T extends AbstractRefCounted> Refs<T> merge(List<Refs<T>> references)
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
    }

    public static <T extends AbstractRefCounted> Refs<T> ref(Iterable<T> reference)
    {
        Map<T, Ref> refs = Refs.ref(reference);
        if (refs == null)
            return null;
        return new Refs<T>(refs);
    }

    public static <T extends AbstractRefCounted> Refs<T> sharedRefs(Iterable<T> reference)
    {
        Map<T, Ref> refs = new HashMap<>();
        for (T t : reference)
            refs.put(t, t.sharedRef());
        return new Refs<T>(refs);
    }

    static boolean seriousLeakDetected()
    {
        return seriousLeak.get();
    }

    // for testing
    private static final ConcurrentHashMap<State, Boolean> softReferences = new ConcurrentHashMap<>();
    private static final AtomicBoolean seriousLeak = new AtomicBoolean();
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
                        if (obj instanceof Link)
                        {
                            ((Link) obj).release(true);
                        }
                        else if (obj instanceof State)
                        {
                            State state = ((State) obj);
                            softReferences.remove(state);
                            state.tidyIfNotAlready();
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
