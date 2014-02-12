package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.OpOrder;

import org.slf4j.*;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


/**
 * See {@link OffHeapCleaner}
 */
public class OffHeapPool extends Pool
{

    private static final Logger logger = LoggerFactory.getLogger(OffHeapPool.class);

    public final int regionSize;
    private volatile boolean isGcEnabled = false;

    /** A queue of regions we have previously used but are now spare */
    private final NonBlockingQueue<OffHeapRegion> recycled = new NonBlockingQueue<>();

    /** The set of live allocators associated with this pool */
    final NonBlockingQueue<SoftReference<OffHeapAllocatorGroup>> groups = new NonBlockingQueue<>();

    public OffHeapPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        this(1 << 20, maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public OffHeapPool(int regionSize, long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
        this.regionSize = regionSize;
    }

    public OffHeapAllocatorGroup newAllocatorGroup(String name, OpOrder reads, OpOrder writes)
    {
        return new OffHeapAllocatorGroup(name, this, reads, writes);
    }

    SubPool getSubPool(boolean onHeap, long limit, float cleanThreshold)
    {
        return onHeap ? super.getSubPool(true, limit, cleanThreshold)
                      : new OffHeapSubPool(limit, cleanThreshold);
    }

    PoolCleanerThread<?> getCleaner(Runnable cleaner)
    {
        return new OffHeapCleaner(this, cleaner);
    }

    /**
     * Either take a region from the recycle queue, or allocate a new one if there is room.
     * If neither is true, return null.
     * @param size
     * @return
     */
    OffHeapRegion tryAllocateRegion(int size)
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
                OffHeapRegion region = recycled.advance();
                if (region != null)
                    return region;
            }
            else if (offHeap.tryAllocate(size))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Allocating new region with size {}, total allocated {}", size, offHeap.allocated());
                return new OffHeapRegion(size, isGcEnabled, size > regionSize);
            }
            else if (!recycled.isEmpty() && size > regionSize)
            {
                // if we're allocating something oversized, and there is room in recycled regions, but not unallocated
                // so free up enough recycled regions to make space for us, and try again
                int reclaim = 0;
                while (reclaim < size)
                {
                    OffHeapRegion region = recycled.advance();
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
    private void free(OffHeapRegion region)
    {
        offHeap.adjustAllocated(-region.size());
        region.releaseNativeMemory();
    }

    /**
     * enable/disable GC for this pool
     * @param enabled true if GC should be enabled
     */
    public void setGcEnabled(boolean enabled)
    {
        assert !enabled || OffHeapRegion.ISGCPOSSIBLE : "OffHeap GC is not possible, as OffHeapRegion could not initialise the necessary machinery. Are you running on a Sun JVM?";
        this.isGcEnabled = enabled;
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
    void recycle(OffHeapRegion region, boolean hasBeenUsed)
    {
        if (region.size() > regionSize || offHeap.isExceeded())
            free(region);
        else
            recycled.append(hasBeenUsed ? region.recycle() : region);
    }

    private final class OffHeapSubPool extends SubPool
    {

        /** The amount of memory allocators from this pool are currently using */
        private final AtomicLong used = new AtomicLong();

        public OffHeapSubPool(long limit, float cleanThreshold)
        {
            super(limit, cleanThreshold);
        }

        public long used()
        {
            return used.get();
        }

        void adjustAcquired(long size, boolean alsoAllocated)
        {
            used.addAndGet(size);
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

}