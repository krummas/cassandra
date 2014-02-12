package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child AbstractAllocator objects. AbstractAllocator and MemoryTracker correspond approximately to PoolAllocator and Pool,
 * respectively, with the MemoryTracker bookkeeping the total shared use of resources, and the AbstractAllocator the amount
 * checked out and in use by a specific PoolAllocator.
 *
 * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
 * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
 * but only needs to allocate if there are none already available. This distinction is not always meaningful.
 */
public abstract class Pool
{
    final PoolCleanerThread<?> cleaner;

    // the total memory used by this pool
    public final SubPool onHeap;
    public final SubPool offHeap;

    final WaitQueue hasRoom = new WaitQueue();

    Pool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanThreshold, Runnable cleaner)
    {
        this.onHeap = getSubPool(true, maxOnHeapMemory, cleanThreshold);
        this.offHeap = getSubPool(false, maxOffHeapMemory, cleanThreshold);
        this.cleaner = getCleaner(cleaner);
        if (this.cleaner != null)
            this.cleaner.start();
    }

    public abstract AllocatorGroup newAllocatorGroup(String name, OpOrder reads, OpOrder writes);

    public OpOrder.Barrier getGCBarrier()
    {
        if (cleaner == null)
            return null;
        return cleaner.getGCBarrier();
    }

    SubPool getSubPool(boolean onHeap, long limit, float cleanThreshold)
    {
        return new SubPool(limit, cleanThreshold);
    }

    PoolCleanerThread<?> getCleaner(Runnable cleaner)
    {
        return cleaner == null ? null : new PoolCleanerThread<>(this, cleaner);
    }

    public class SubPool
    {

        // total memory/resource permitted to allocate
        public final long limit;

        // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
        public final float cleanThreshold;

        // total bytes allocated and reclaiming
        private final AtomicLong allocated = new AtomicLong();
        private final AtomicLong reclaiming = new AtomicLong();

        // a cache of the calculation determining at what allocation threshold we should next clean
        private final AtomicLong nextClean = new AtomicLong();

        public SubPool(long limit, float cleanThreshold)
        {
            this.limit = limit;
            this.cleanThreshold = cleanThreshold;
        }

        /** Methods for tracking and triggering a clean **/

        boolean needsCleaning()
        {
            // use strictly-greater-than so we don't clean when limit is 0
            return used() > nextClean.get() && updateNextClean();
        }

        void maybeClean()
        {
            if (needsCleaning() && cleaner != null)
                cleaner.trigger();
        }

        private boolean updateNextClean()
        {
            while (true)
            {
                long current = nextClean.get();
                long reclaiming = this.reclaiming.get();
                long next =  reclaiming + (long) (this.limit * cleanThreshold);
                if (current == next || nextClean.compareAndSet(current, next))
                    return used() > next;
            }
        }

        /** Methods to allocate space **/

        boolean tryAllocate(int size)
        {
            while (true)
            {
                long cur;
                if ((cur = allocated.get()) + size > limit)
                    return false;
                if (allocated.compareAndSet(cur, cur + size))
                    return true;
            }
        }

        /**
         * apply the size adjustment to allocated, bypassing any limits or constraints. If this reduces the
         * allocated total, we will signal waiters
         */
        void adjustAllocated(long size)
        {
            if (size == 0)
                return;
            while (true)
            {
                long cur = allocated.get();
                if (allocated.compareAndSet(cur, cur + size))
                    return;
            }
        }

        // 'acquires' an amount of memory, and maybe also marks it allocated. This method is meant to be overridden
        // by implementations with a separate concept of acquired/allocated. As this method stands, an acquire
        // without an allocate is a no-op (acquisition is achieved through allocation), however a release (where size < 0)
        // is always processed and accounted for in allocated.
        void adjustAcquired(long size, boolean alsoAllocated)
        {
            if (size > 0 || alsoAllocated)
            {
                if (alsoAllocated)
                    adjustAllocated(size);
                maybeClean();
            }
            else if (size < 0)
            {
                adjustAllocated(size);
                hasRoom.signalAll();
            }
        }

        // space reclaimed should be released prior to calling this, to avoid triggering unnecessary cleans
        void adjustReclaiming(long reclaiming)
        {
            if (reclaiming == 0)
                return;
            this.reclaiming.addAndGet(reclaiming);
            if (reclaiming < 0 && updateNextClean() && cleaner != null)
                cleaner.trigger();
        }

        public boolean isExceeded()
        {
            return allocated.get() > limit;
        }

        public long allocated()
        {
            return allocated.get();
        }

        public long used()
        {
            return allocated.get();
        }

        public long reclaiming()
        {
            return reclaiming.get();
        }

        public PoolAllocator.SubAllocator newAllocator()
        {
            return new PoolAllocator.SubAllocator(this);
        }

        public WaitQueue hasRoom()
        {
            return hasRoom;
        }
    }

}
