package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.OpOrder;

import org.slf4j.*;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


public class NativePool extends Pool
{

    private static final Logger logger = LoggerFactory.getLogger(NativePool.class);

    public final int regionSize;
    final int overAllocatedRegionSize; // size of region to allocate if we're full but need temporary room
    private volatile boolean isGcEnabled = false;

    /** A queue of regions we have previously used but are now spare */
    private final NonBlockingQueue<NativeRegion> recycled = new NonBlockingQueue<>();

    /** The set of live allocators associated with this pool */
    final NonBlockingQueue<SoftReference<NativePoolGroup>> groups = new NonBlockingQueue<>();

    public NativePool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        this(1 << 20, maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public NativePool(int regionSize, long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
        this.regionSize = regionSize;
        this.overAllocatedRegionSize = Math.max(regionSize >> 8, Math.min(regionSize, 8 << 10));
    }

    public NativePoolGroup newAllocatorGroup(String name, OpOrder reads, OpOrder writes)
    {
        return new NativePoolGroup(name, this, reads, writes);
    }

    SubPool getSubPool(boolean onHeap, long limit, float cleanThreshold)
    {
        return onHeap ? super.getSubPool(true, limit, cleanThreshold)
                      : new OffHeapSubPool(limit, cleanThreshold);
    }

    PoolCleanerThread<?> getCleaner(Runnable cleaner)
    {
        return new NativeCleaner(this, cleaner);
    }

    /**
     * Either take a region from the recycle queue, or allocate a new one if there is room.
     * If neither is true, return null.
     * @param size
     * @return
     */
    NativeRegion tryAllocateRegion(int size)
    {
        while (true)
        {
            /**
             * tryAllocate encapsulates knowledge of if we can satisfy our request without allocating
             * success means we need to allocate a new region; failure means we may be able to satisfy
             * from our pool, or that we shouldn't wait
             */
            if (!recycled.isEmpty() && size == regionSize)
            {
                // try returning a recycled region
                NativeRegion region = recycled.advance();
                if (region != null)
                    return region;
            }
            else if (offHeap.tryAllocate(size))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Allocating new region with size {}, total allocated {}", size, offHeap.allocated());
                return new NativeRegion(size, isGcEnabled);
            }
            else if (!recycled.isEmpty() && size > regionSize)
            {
                // if we're allocating something oversized, and there is room in recycled regions, but not unallocated
                // so free up enough recycled regions to make space for us, and try again
                int reclaim = 0;
                while (reclaim < size)
                {
                    NativeRegion region = recycled.advance();
                    if (region == null)
                        break;
                    free(region);
                    reclaim += regionSize;
                }
            }
            else
                return null;

        }
    }

    /**
     * deallocate the region
     *
     * @param region
     */
    private void free(NativeRegion region)
    {
        offHeap.adjustAllocated(-region.size());
        region.releaseNativeMemory();
    }

    /**
     * enable/disable GC for this pool
     * @param enabled true if GC should be enabled
     */
    public NativePool setGcEnabled(boolean enabled)
    {
        this.isGcEnabled = enabled;
        return this;
    }

    public boolean isGcEnabled()
    {
        return this.isGcEnabled;
    }

    /**
     * Either put the region back in the pool, or free it up if it's oversized or we're over limit.
     * When discarding an OffHeapAllocator we release the memory ownership before we recycle here,
     * so no need to account for it. We are simply reusing the region.
     *
     * Note that the region provided should be region.recycle() unless it was never exposed.
     *
     * @param region
     */
    void recycle(NativeRegion region, boolean hasBeenUsed)
    {
        if (region.size() != regionSize || offHeap.isExceeded())
            free(region);
        else
            recycled.append(hasBeenUsed ? region.recycle() : region);
    }

    /**
     * A collection of allocators that are logically grouped, and which can share OpOrder guarding
     * GC root exploration is done at the group level, which map to a CFS basically, because most
     * operations are contained within one CFS we only need to explore the roots for the CFS we're collecting
     */
    public class Group extends Pool.AllocatorGroup<NativePool>
    {
        public Group(String name, NativePool parent, OpOrder reads, OpOrder writes)
        {
            super(name, parent, reads, writes);
        }

        public NativePoolAllocator newAllocator()
        {
            return new NativePoolAllocator(this);
        }
    }

    private final class OffHeapSubPool extends SubPool
    {

        /** The amount of memory allocators from this pool are currently using */
        volatile long used;

        public OffHeapSubPool(long limit, float cleanThreshold)
        {
            super(limit, cleanThreshold);
        }

        public long used()
        {
            return used;
        }

        void adjustAcquired(long size, boolean alsoAllocated)
        {
            usedUpdater.addAndGet(this, size);
            if (size >= 0)
            {
                if (alsoAllocated)
                    adjustAllocated(size);
                maybeClean();
            }
            else
            {
                hasRoom.signalAll();
            }
        }
    }

    private static final AtomicLongFieldUpdater<OffHeapSubPool> usedUpdater = AtomicLongFieldUpdater.newUpdater(OffHeapSubPool.class, "used");
}