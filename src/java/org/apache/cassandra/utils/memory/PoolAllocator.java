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
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class PoolAllocator<G extends AllocatorGroup<P>, P extends Pool> extends AbstractAllocator
{

    public final G group;
    public final SubAllocator onHeap;
    public final SubAllocator offHeap;
    volatile State state = State.get(LifeCycle.LIVE, Gc.INACTIVE);

    static final AtomicReferenceFieldUpdater<PoolAllocator, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(PoolAllocator.class, State.class, "state");

    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED
    }
    static enum Gc
    {
        INACTIVE, REALLOCATING, COLLECTING, FORBIDDEN
    }

    // we mix in life-cycle information for off-heap allocators here, because it's neater than specialising there
    static final class State
    {
        final LifeCycle lifeCycle;
        final Gc gc;

        // Cache not all of the possible combinations of LifeCycle/Gc.
        // Not all of these states are valid, but easier to just create them all.
        private static final State[] ALL;
        private static final int MULT;
        static
        {
            LifeCycle[] lifeCycles = LifeCycle.values();
            Gc[] gcs = Gc.values();
            ALL = new State[lifeCycles.length * gcs.length];
            for (int i = 0 ; i < lifeCycles.length ; i++)
                for (int j = 0 ; j < gcs.length ; j++)
                    ALL[(i * gcs.length) + j] = new State(lifeCycles[i], gcs[j]);
            MULT = gcs.length;
        }

        private State(LifeCycle lifeCycle, Gc gc)
        {
            this.lifeCycle = lifeCycle;
            this.gc = gc;
        }

        private static State get(LifeCycle lifeCycle, Gc gc)
        {
            return ALL[(lifeCycle.ordinal() * MULT) + gc.ordinal()];
        }

        /**
         * maybe transition to the requested Gc state, depending on the current state.
         */
        State transition(Gc targetState)
        {
            switch (targetState)
            {
                case REALLOCATING:
                    // we only permit entering the marking state if GC is not already running for this allocator
                    if (gc != Gc.INACTIVE)
                        return null;
                    // we don't permit GC on an allocator we're discarding, or have discarded
                    if (lifeCycle.compareTo(LifeCycle.DISCARDING) >= 0)
                        return null;
                    return get(lifeCycle, Gc.REALLOCATING);
                case COLLECTING:
                    assert gc == Gc.REALLOCATING;
                    return get(lifeCycle, Gc.COLLECTING);
                case INACTIVE:
                    assert gc == Gc.COLLECTING;
                    if (lifeCycle.compareTo(LifeCycle.DISCARDING) >= 0)
                        return get(lifeCycle, Gc.FORBIDDEN);
                    return get(lifeCycle, Gc.INACTIVE);
            }
            throw new IllegalStateException();
        }

        State transition(LifeCycle targetState)
        {
            switch (targetState)
            {
                case DISCARDING:
                    assert lifeCycle == LifeCycle.LIVE;
                    return get(LifeCycle.DISCARDING, gc);
                case DISCARDED:
                    assert lifeCycle == LifeCycle.DISCARDING;
                    return get(LifeCycle.DISCARDED, gc);
            }
            throw new IllegalStateException();
        }

        public String toString()
        {
            return lifeCycle + ", GC:" + gc;
        }

    }

    PoolAllocator(G group)
    {
        this.group = group;
        this.onHeap = group.pool.onHeap.newAllocator();
        this.offHeap = group.pool.offHeap.newAllocator();
    }

    /**
     * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void setDiscarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        state = state.transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        onHeap.releaseAll();
        offHeap.releaseAll();
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    /** Mark the BB as unused, permitting it to be reclaimed */
    public abstract void free(ByteBuffer name);

    public boolean isLive()
    {
        return state.lifeCycle == LifeCycle.LIVE;
    }

    /**
     * Allocate a slice of the given length.
     */
    public ByteBuffer clone(ByteBuffer buffer, OpOrder.Group opGroup)
    {
        assert buffer != null;
        if (buffer.remaining() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer cloned = allocate(buffer.remaining(), opGroup);

        cloned.mark();
        cloned.put(buffer.duplicate());
        cloned.reset();
        return cloned;
    }

    public ContextAllocator wrap(OpOrder.Group opGroup, ColumnFamilyStore cfs)
    {
        return new ContextAllocator(opGroup, this, cfs);
    }


    /**
     * Created by a SubPool to represent memory that is temporarily allocated to this allocator.
     * When the allocator needs more memory, it allocates it through this object, which acquires
     * (and maybe allocates) memory in its parent SubPool and accounts for it here as well. Once the allocator is done
     * it relinquishes the resources through this class, which ensures the resources are freed in the parent SubPool.
     */
    public static final class SubAllocator
    {
        // the tracker we are owning memory from
        private final Pool.SubPool parent;

        // the amount of memory/resource owned by this object
        private final AtomicLong owns = new AtomicLong();
        // the amount of memory we are reporting to collect; this may be inaccurate, but is close
        // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
        private final AtomicLong reclaiming = new AtomicLong();

        SubAllocator(Pool.SubPool parent)
        {
            this.parent = parent;
        }

        // should only be called once we know we will never allocate to the object again.
        // currently no corroboration/enforcement of this is performed.
        void releaseAll()
        {
            parent.adjustAcquired(-owns.getAndSet(0), false);
            parent.adjustReclaiming(-reclaiming.getAndSet(0));
        }

        // allocate memory in the tracker, and mark ourselves as owning it
        public void allocate(int size, OpOrder.Group opGroup)
        {
            while (true)
            {
                if (parent.tryAllocate(size))
                {
                    acquired(size);
                    return;
                }
                WaitQueue.Signal signal = opGroup.safeIsBlockingSignal(parent.hasRoom().register());
                boolean allocated = parent.tryAllocate(size);
                if (allocated || opGroup.isBlocking())
                {
                    signal.cancel();
                    if (allocated) // if we allocated, take ownership
                        acquired(size);
                    else // otherwise we're blocking so we're permitted to overshoot our constraints, to just allocate without blocking
                        allocated(size);
                    return;
                }
                else
                    signal.awaitUninterruptibly();
            }
        }

        // retroactively mark an amount allocated amd acquired in the tracker, and owned by us
        void allocated(int size)
        {
            parent.adjustAcquired(size, true);
            owns.addAndGet(size);
        }

        // retroactively mark an amount acquired in the tracker, and owned by us
        void acquired(int size)
        {
            parent.adjustAcquired(size, false);
            owns.addAndGet(size);
        }

        void release(int size)
        {
            parent.adjustAcquired(-size, false);
            owns.addAndGet(-size);
        }

        // if this.owns > size, subtract size from this.owns (atomically), otherwise return false
        boolean transfer(int size)
        {
            while (true)
            {
                long cur = owns.get();
                long next = cur - size;
                if (next < 0)
                    return false;
                if (owns.compareAndSet(cur, next))
                    return true;
            }
        }

        // subtract at most size from this.reclaiming (atomically), and adjust the parent's reclaiming by
        // the difference between what we wanted to subtract and we actually subtracted.
        // this is used to atomically correct the reclaiming amount after it was speculatively set before an absolute
        // figure could be established, and to take ownership of the reclamation away from this allocator for delayed recycling
        void transferReclaiming(int size)
        {
            while (true)
            {
                long cur = reclaiming.get();
                long next = Math.max(0, cur - size);
                if (cur == 0 || reclaiming.compareAndSet(cur, next))
                {
                    parent.adjustReclaiming(size - (cur - next));
                    return;
                }
            }
        }

        // must be called with exclusive access, but safe to call multiple times
        void markAllReclaiming()
        {
            while (true)
            {
                long cur = owns.get();
                long prev = reclaiming.get();
                if (reclaiming.compareAndSet(prev, cur))
                {
                    parent.adjustReclaiming(cur - prev);
                    return;
                }
            }
        }

        public long owns()
        {
            return owns.get();
        }

        public long reclaiming()
        {
            return reclaiming.get();
        }

        public float ownershipRatio()
        {
            return owns.get() / (float) parent.limit;
        }

        public WaitQueue.Signal hasRoomSignal()
        {
            return parent.hasRoom().register();
        }

    }

}