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

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.nio.ByteBuffer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This allocator uses native memory, with a complex life-cycle explained in {@link OffHeapCleaner}
 *
 * Here we will discuss the life/state-cycle of the allocator itself only.
 * There are two parallel state-cycles: Gc and LifeCycle. LifeCycle is a linear transition managing
 * the disposal of an allocator, whereas Gc is a repeatable cycle managing the phases of Gc, obviously.
 *
 * LifeCycle:
 *  LIVE         - The allocator is in use
 *  DISCARDING   - The allocator will be discarded shortly. It is expected that during this phase
 *                 allocations gradually cease. In practice this is enforced by redirecting all new writes
 *                 to a new replacement memtable/allocator; so once earlier writes have finished we can discard.
 *                 Once we start discarding we trigger a Gc, and from that point forbid any future Gc.
 *  DISCARDED    - The allocator is no longer ostensibly in use. No more allocations may be performed against it,
 *                 and the resources it holds will *eventually* be returned for use by the pool.
 *                 Resources are returned under the same conditions as recycling regions freed up by GC in {@link OffHeapCleaner}
 * Gc:
 *  INACTIVE     - No Gc is in progress. We may transition to REALLOCATING at any time.
 *  REALLOCATING - We have started a Gc, and are currently reallocating the still live references from any
 *                 regions we intend to recycle
 *  COLLECTING   - We have finished reallocating, and are now either compacting data we are keeping, are marking
 *                 those references that are still in use, or are collecting any regions we have freed.  Note that there
 *                 may be delayed recycles associated with this allocator after exiting this state
 *  FORBIDDEN    - No Gc is in progress, and no Gc (or change of Gc state) is now permitted. Entered when
 *                 exiting COLLECTING if we are DISCARDING or DISCARDED.
 */
public class OffHeapAllocator extends PoolAllocator<OffHeapAllocatorGroup, OffHeapPool>
{

    /** The Regions we are currently allocating from - we don't immediately move forwards all allocations to a new Region
     * if any one allocation fails, instead we only advance if there is a small fraction of the total room left, or we
     * have failed a number of times to allocate to the Region. This should help prevent wasted space.*/
    final NonBlockingQueue<OffHeapRegion> allocatingFrom = new NonBlockingQueue<>();
    final NonBlockingQueueView<OffHeapRegion> allocated = allocatingFrom.view();

    OffHeapAllocator(OffHeapAllocatorGroup group)
    {
        super(group);
    }

    /**
     * it would be possible to permit operations here relatively easily, but we would need to briefly permit
     * unbounded growth for these ops during GC to prevent deadlock, or some other trick. Since that is not
     * necessary for the moment, we stick with the safer / more correct version.
     */
    public ByteBuffer allocate(int size)
    {
        throw new UnsupportedOperationException();
    }

    public ByteBuffer allocate(int size, OpOrder.Group writeOp)
    {

        assert size >= 0;
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        while (true)
        {
            ByteBuffer alloc = tryAllocate(size);
            if (alloc != null)
                return alloc;

            WaitQueue.Signal signal = writeOp.safeIsBlockingSignal(offHeap.hasRoomSignal());

            alloc = tryAllocate(size);
            if (alloc != null || writeOp.isBlocking())
            {
                signal.cancel();
                if (alloc == null)
                {
                    // if we're out of room, and some downstream process is blocking on us,
                    // just recklessly allocate from the heap and continue
                    alloc = ByteBuffer.allocate(size);
                    onHeap.allocated(size);
                }
                return alloc;
            }
            else
            {
                group.pool.cleaner.forceClean();
                signal.awaitUninterruptibly();
            }
        }
    }

    private ByteBuffer tryAllocate(int size)
    {
        OffHeapPool pool = group.pool;

        if (size > pool.regionSize)
        {
            OffHeapRegion region = pool.tryAllocateRegion(size);
            if (region == null)
                return null;
            ByteBuffer r = region.allocate(size, true);
            offHeap.acquired(size);
            adopt(region);
            return r;
        }

        while (true)
        {
            // attempt to allocate from any regions we consider to have space left in them
            OffHeapRegion last = allocatingFrom.tail();
            for (OffHeapRegion region : allocatingFrom)
            {
                last = region;

                ByteBuffer allocated = region.allocate(size, true);
                if (allocated != null)
                    return allocated;

                // if we failed to allocate, maybe remove ourselves from the
                // collection of regions we try to allocate from in future
                if (!region.mayContinueAllocating())
                    if (allocatingFrom.advanceIfHead(region))
                        region.finishAllocating();
            }

            // if we fail, allocate a new region either from the pool, or directly
            OffHeapRegion newRegion = pool.tryAllocateRegion(pool.regionSize);
            if (newRegion == null)
                return null;

            // try to append to our list - if the list has been appended to already, put it back in the pool
            // and try allocating from the Region somebody beat us to adding, otherwise set ourselves as its owner
            offHeap.acquired(pool.regionSize);
            if (!allocatingFrom.appendIfTail(last, newRegion))
            {
                offHeap.release(pool.regionSize);
                pool.recycle(newRegion, false);
            }
        }
    }

    void adopt(OffHeapRegion region)
    {
        allocatingFrom.append(region);
    }

    /**
     * atomically transition to targetLifecycle (always), and targetGc (if possible).
     * this is used by setDiscarding() to ensure no future GCs occur after the one currently running (if any)
     * or the one started by setDiscarding (otherwise).
     *
     * If the switch to Marking fails it means we are already GCing; once we are set Discarding any Gc not already
     * "started" (as declared by the state) will be forbidden. So we want to ensure that when we set Discarding
     * the Gc state is >= Marking. When finishing the final we are now guaranteed to be in the Discarding state,
     * so we transition to Gc.Forbidden instead of Inactive.
     *
     * @param targetGc the target Gc state (only applied when current state permits)
     * @param targetLifecycle the target LifeCycle state (always applied_
     * @return true if we successfully switched to targetGc, and false if we only switched targetLifecycle
     */
    boolean transition(Gc targetGc, LifeCycle targetLifecycle)
    {
        while (true)
        {
            State cur = state;
            State next = cur.transition(targetGc);
            boolean success = next != null;
            if (!success)
            {
                if (targetLifecycle == null)
                    return false;
                next = cur.transition(targetLifecycle);
            }
            else if (targetLifecycle != null)
            {
                next = next.transition(targetLifecycle);
            }
            if (cur == next || stateUpdater.compareAndSet(this, cur, next))
                return success;
        }
    }

    boolean transition(Gc targetState)
    {
        return transition(targetState, null);
    }

    void transition(LifeCycle targetState)
    {
        while (true)
        {
            State cur = state;
            State next = cur.transition(targetState);
            if (stateUpdater.compareAndSet(this, cur, next))
                return;
        }
    }

    /**
     * Mark this allocator reclaiming; this will allow any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void setDiscarding()
    {
        group.live.remove(this);
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();

        if (!group.pool.isGcEnabled())
            transition(LifeCycle.DISCARDING);
        else if (transition(Gc.REALLOCATING, LifeCycle.DISCARDING))
            OffHeapCleaner.unsafeGc(this);
    }

    public void setDiscarded()
    {
        transition(LifeCycle.DISCARDED);
        doDiscard();
    }

    // we permit GC to repeat the discard work, in case it finished after we discarded,
    // as it may have given us some new regions
    void doDiscard()
    {
        OpOrder.Barrier readBarrier = group.reads.newBarrier();
        readBarrier.issue();

        // place all regions that aren't already being discarded (say, by GC) into a map that will be used
        // to mark them referenced by any read ops started before our barrier
        final Multimap<byte[], OffHeapDelayedRecycle> markLookup = ArrayListMultimap.create(2 * (int) (offHeap.owns() / group.pool.regionSize), 1);
        for (OffHeapRegion region : allocated)
        {
            if (region.transition(OffHeapRegion.State.LIVE, OffHeapRegion.State.DISCARDING))
            {
                markLookup.put(region.markKey, new OffHeapDelayedRecycle(group.pool, region));
                if (!offHeap.transfer(region.size()))
                    throw new AssertionError();
                offHeap.transferReclaiming(region.size());
            }
        }

        if (!markLookup.isEmpty())
        {
            readBarrier.await();
            // must wait until after the read barrier to ensure all referrers are populated
            group.mark(readBarrier, markLookup);

            for (OffHeapDelayedRecycle marked : markLookup.values())
                marked.unmark();
        }

        // by recycling before releasing the memory accounting, we don't need to worry about waking up explicitly here
        onHeap.releaseAll();
    }

    /**
     * Mark the buffer as no longer used, potentially permitting its space to be reclaimed
     *
     * @param buffer the buffer to free
     */
    public void free(ByteBuffer buffer)
    {
        if (!buffer.isDirect())
        {
            // we sometimes allocate heap buffers if we run out of memory
            onHeap.release(buffer.capacity());
            return;
        }
        if (!group.pool.isGcEnabled())
            return;
        if (!OffHeapRegion.free(buffer))
            throw new IllegalStateException();
    }

}
