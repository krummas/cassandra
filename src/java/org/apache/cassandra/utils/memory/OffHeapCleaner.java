package org.apache.cassandra.utils.memory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OptBlockingQueue;
import org.apache.cassandra.utils.concurrent.SafeRemoveIterator;
import org.apache.mina.util.IdentityHashSet;

import org.slf4j.*;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import static org.apache.cassandra.utils.memory.OffHeapRegion.StateSnapper;

/**
 * This class encapsulates the main body of functionality for collecting garbage from an {@link OffHeapPool}
 *
 * Memory in a pool is managed through four (yes, four) different mechanisms in its lifecycle:
 *
 * 1) Initially memory is managed like malloc/free: it is considered referenced at least until the allocation is
 *    matched by a corresponding free()
 * 2) The memory is then protected by the associated OpOrder(s); an allocation is only available for collection
 *    once all operations started prior to the free() have completed
 * 3) During this time any operation protected by the read/write OpOrder may optionally 'ref' an object, or objects, that
 *    each reference (in the normal java sense) some allocations; once the read operation finishes these will have been
 *    registered with one or more GC roots (one per allocator group referenced).
 * 4) When a GC (or allocator discard) occurs, the extant 'refs' that were created during operations started prior
 *    to the collect phase are walked, and any regions that are reachable by any of these 'refs' are switched to
 *    a refcount phase. When each ref completes it decrements the count of any such regions it reached.
 *    Once this count hits 0, the region is finally eligible for reuse.
 *
 * Garbage collection is composed of the following phases:
 *
 * Candidate selection:
 *    Run over either the pool or one allocator, it partially sorts the regions in order of
 *    memory that can be reclaimed. Only regions that have already been fully allocated are considered as candidates.
 * Reallocation:
 *    The candidates are walked from best-to-worst, and a GC thread is started for each allocator we encounter.
 *    For each candidate, if there is space either in the allocator's GC state, or in the pool, to compact its still live
 *    (not freed) allocations, then a new (re)allocation is made for them and they are passed to the collector thread to process.
 * Compaction:
 *    The GC thread for each allocator waits until any prior write operations are completed before proceeding to copy
 *    any retained allocations to the new locations assigned during the previous phase. The original objects are updated
 *    to point to the new locations, and their mark key (used in next phase) is updated to point to the new region.
 * Marking:
 *    Once all compaction is done, we wait for any prior read operations to complete, by which point we reach stage (3)
 *    of the lifecycle of any of the allocations not compacted. Now we walk any 'refs' that may reference allocations
 *    we're collecting (i.e. those that are registered with the {@link OffHeapAllocatorGroup} of the allocator we're GCing)
 *    and register any of the regions we're recycling with the Referrer, whilst incrementing the ref-count on the region.
 * Delayed? Recycle:
 *    Any regions that were not ref-count-incremented during the mark phase are immediately recycled, and any that were
 *    enter a limbo period, until the reference is cleared.
 *
 * Garbage collection is triggered in one of three ways:
 *
 * 1) One of the pool clean thresholds is breached, so the cleaner thread is triggered; in this instance the private
 *    gc(Pool) method is used, which scans all allocators and groups for the best candidates to collect
 * 2) An allocator completely fails to allocate new memory. In this case before blocking we attempt a GC. This is
 *    potentially an expensive waste of time, but since there is no more memory to allocate for writes, it's probably
 *    time that would otherwise be unused, and may result in some freed memory.
 * 3) An allocator is marked discardING. This is currently triggered by a memtable flush. In this instance we
 *    force a GC if one isn't already running, and perform the mark phase in the discarding thread.
 *
 * Some notes:
 * We assume that writers never create references that persist outside of the life of their OpOrder transaction
 *
 */

class OffHeapCleaner extends PoolCleanerThread<OffHeapPool>
{

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCleaner.class);

    private static final ExecutorService collectors = Executors.newCachedThreadPool(new NamedThreadFactory("OffHeapGC"));
    private static final ConcurrentHashMap<OffHeapAllocator, GarbageCollector> running = new ConcurrentHashMap<>();
    private static final OpOrder gcOperations = new OpOrder();

    public OffHeapCleaner(OffHeapPool pool, Runnable clean)
    {
        super(pool, clean);
    }

    void clean()
    {
        if (pool.isGcEnabled())
            gc(pool, false);
        if (!pool.isGcEnabled() || needsCleaning())
            super.clean();
    }

    // force an out-of-band gc; only collect anything we can collect for free, since we call this when under memory pressure
    void forceClean()
    {
        gc(pool, true);
    }

    // wait for any running GCs to finish
    public OpOrder.Barrier getGCBarrier()
    {
        return gcOperations.newBarrier();
    }

    /**
     * Perform a full GC cycle on the provided pool
     *
     * @param pool the pool to GC
     * @param freeToCollectOnly if true, we only try to collect those regions that can be collected without compaction
     * (i.e. are completely free)
     */
    private static void gc(OffHeapPool pool, boolean freeToCollectOnly)
    {
        // we set success to false as soon as we fail to allocate room for migrating regions, and from then only collect
        boolean success = true;
        List<Candidate> candidates = Candidate.get(pool, freeToCollectOnly);
        if (logger.isTraceEnabled())
            logger.trace("Starting Global GC with {} candidates", candidates.size());
        Map<OffHeapAllocator, GarbageCollector> collectors = new HashMap<>();
        Set<OffHeapAllocator> couldNotStart = new HashSet<>();
        /**
         * loop through all candidates in descending order of amount we expect to reclaim;
         * as soon as we fail to allocate collection space for an allocator we stop starting
         * new collectors and simply attempt to utilise any remaining space available to us
         * in the existing collectors, closing them as they fail to accept more work.
         */
        for (Candidate candidate : candidates)
        {
            GarbageCollector gc = collectors.get(candidate.allocator);
            if (gc == null)
            {
                // only allocate a new collector if all previous selections have succeeded
                // (otherwise we're probably still out of room)
                if (!success)
                    continue;
                // don't want to start a collection at the bottom end of what we can collect;
                // if we fail to start a new collection first time around, leave it until next time
                if (couldNotStart.contains(candidate.allocator))
                    continue;
                gc = GarbageCollector.start(candidate.allocator, true);
                if (gc == null)
                {
                    couldNotStart.add(candidate.allocator);
                    continue;
                }
                collectors.put(candidate.allocator, gc);
            }
            if (!gc.reallocate(candidate.region))
            {
                success = false;
                gc.finishReallocating();
                collectors.remove(gc.allocator);
                if (collectors.isEmpty())
                    break;
            }
        }
        for (GarbageCollector gc : collectors.values())
            gc.finishReallocating();
    }

    /** Forces a GC of the allocator. Does not affect or check transition state, so must be protected outside of call. */
    static void unsafeGc(OffHeapAllocator allocator)
    {
        GarbageCollector collector = GarbageCollector.start(allocator, false);
        for (Candidate candidate : Candidate.get(allocator, false))
            if (!collector.reallocate(candidate.region))
                break;
        collector.finishReallocating();
    }

    // CANDIDATE SELECTION PHASE:

    // see header description
    private static final class Candidate
    {
        final OffHeapRegion region;
        final OffHeapAllocator allocator;
        private Candidate(OffHeapRegion region, OffHeapAllocator allocator)
        {
            this.region = region;
            this.allocator = allocator;
        }

        // when selecting candidates, we bucket them to vaguely similar ratios of expected collection result,
        // and just select from descending sized buckets in the order we originally visited. this is almost as good
        // as sorting, but much cheaper. here we also define our lowest free ratio necessary to trigger a collection
        private static final float[] DEFAULT_BUCKET_RATIOS = new float[] { 0.1f, 0.25f, 0.5f, 0.8f, 0.99f };
        private static final float[] ONLY_FREE_RATIOS = new float[] { 1.0f };

        private static List<Candidate> get(OffHeapPool pool, boolean freeToCollectOnly)
        {
            float[] ratios = freeToCollectOnly ? ONLY_FREE_RATIOS : DEFAULT_BUCKET_RATIOS;
            List<Candidate>[] buckets = buckets(ratios);
            Iterator<SoftReference<OffHeapAllocatorGroup>> iter = pool.groups.iterator();
            while (iter.hasNext())
            {
                OffHeapAllocatorGroup group = iter.next().get();
                if (group == null)
                {
                    iter.remove();
                    continue;
                }
                for (OffHeapAllocator allocator : group.live.keySet())
                    addCandidates(allocator, buckets, ratios);
            }
            return flatten(buckets);
        }

        private static List<Candidate> get(OffHeapAllocator allocator, boolean freeToCollectOnly)
        {
            float[] ratios = freeToCollectOnly ? ONLY_FREE_RATIOS : DEFAULT_BUCKET_RATIOS;
            return flatten(addCandidates(allocator, buckets(ratios), ratios));
        }

        private static List<Candidate>[] buckets(float[] ratios)
        {
            final List<Candidate>[] buckets = new List[ratios.length];
            for (int i = 0 ; i < buckets.length ; i++)
                buckets[i] = new ArrayList<>();
            return buckets;
        }

        private static List<Candidate> flatten(List<Candidate>[] buckets)
        {
            final List<Candidate> flat = new ArrayList<>();
            for (int i = buckets.length - 1 ; i >= 0 ; i--)
                flat.addAll(buckets[i]);
            return flat;
        }

        private static List<Candidate>[] addCandidates(OffHeapAllocator allocator, List<Candidate>[] buckets, float[] ratios)
        {
            // TODO: don't consider a region a candidate if it contains allocations copied from a prior region
            // that have not been released yet (so we don't slowly spread a stuck allocation)
            Iterator<OffHeapRegion> iter = allocator.allocated.iterator();
            while (iter.hasNext())
            {
                OffHeapRegion region = iter.next();
                if (region.isAllocating())
                    continue;
                if (region.isLive())
                {
                    float freeRatio = region.freeRatio();
                    int pos = Arrays.binarySearch(ratios, freeRatio);
                    if (pos < 0)
                        pos = -1 - pos;
                    pos -= 1;
                    if (pos >= 0)
                        buckets[pos].add(new Candidate(region, allocator));
                }
            }
            return buckets;
        }

    }

    // REALLOCATION AND COMPACTION PHASES:

    /**
     * A collection of retained allocations representing all live allocations from a region we are reclaiming
     */
    private static final class CompactRegion
    {
        final OffHeapRegion regionIn;
        final List<Move> moves = new ArrayList<>();
        private CompactRegion(OffHeapRegion regionIn)
        {
            this.regionIn = regionIn;
        }

        /**
         * A single live allocation retention: from a region we are reclaiming, into a preallocated position in a target region
         */
        private static final class Move
        {
            final ByteBuffer in, out;
            final OffHeapRegion regionOut;
            private Move(ByteBuffer in, ByteBuffer out, OffHeapRegion regionOut)
            {
                this.in = in;
                this.out = out;
                this.regionOut = regionOut;
            }
        }
    }

    /**
     * A GC runnable operating over a single allocator; one is spawned per allocator with regions
     * targeted for collection/compaction. The mark() operation is performed (usually) by the cleaner gc() call,
     * which decides what memory to keep and allocates new space for it before placing it on the compact queue
     */
    private static final class GarbageCollector implements Runnable
    {
        final OffHeapAllocator allocator;

        // mark state
        final NonBlockingQueue<StateSnapper> allocatingFrom = new NonBlockingQueue<>(); // regions we're compacting to with space free
        final NonBlockingQueueView<StateSnapper> allocatedFrom = allocatingFrom.view(); // all regions we've compacted to
        final CountDownLatch doneMark = new CountDownLatch(1);

        final OptBlockingQueue<CompactRegion> compact = new OptBlockingQueue<>();       // compaction work
        final NonBlockingQueue<OffHeapRegion> collect = new NonBlockingQueue<>();       // all regions we're collecting (superset of those in compact.regionIn)
        private long reclaiming = 0;

        GarbageCollector(OffHeapAllocator allocator)
        {
            this.allocator = allocator;
        }

        static GarbageCollector start(OffHeapAllocator allocator, boolean safe)
        {
            if (safe && !allocator.transition(PoolAllocator.Gc.REALLOCATING))
                return null;
            assert !running.containsKey(allocator);
            if (logger.isTraceEnabled())
                logger.trace("Starting GC of allocator{}", System.identityHashCode(allocator));
            GarbageCollector collector = new GarbageCollector(allocator);
            running.put(allocator, collector);
            collectors.execute(collector);
            return collector;
        }

        // REALLOCATION PHASE

        boolean reallocate(OffHeapRegion region)
        {
            assert !region.isAllocating();

            // move the region to GC state - if we fail this could be because we're still allocating from the region
            // or the allocator is being discarded, or because we previously GC'd it and failed to remove, so just return
            if (!region.transition(OffHeapRegion.State.LIVE, OffHeapRegion.State.DISCARDING))
                return true;

            if (region.isAllFree())
            {
                // don't bother marking children if we can collect everything
                adjustReclaiming(region);
                collect.append(region);
                return true;
            }

            // we speculatively allocate in the available regions, so we snap the current state in case we fail
            for (StateSnapper newRegion : allocatingFrom)
                newRegion.snap();

            boolean success = true;
            final CompactRegion compact = new CompactRegion(region);
            for (ByteBuffer copy : region.children())
            {
                // skip any that have been freed/moved to another region
                Object parent = OffHeapRegion.getParent(copy);
                if (parent == null)
                    continue;
                assert parent == region;

                ByteBuffer replacement = null;
                OffHeapRegion regionOfReplacement = null;
                // try and allocate in our local pool
                for (StateSnapper snapper : allocatingFrom)
                {
                    replacement = snapper.region.allocate(copy.limit(), false);
                    if (replacement != null)
                    {
                        regionOfReplacement = snapper.region;
                        break;
                    }
                }
                if (replacement == null)
                {
                    // have to allocate a new region if possible
                    regionOfReplacement = allocator.group.pool.tryAllocateRegion(allocator.group.pool.regionSize);
                    if (regionOfReplacement == null)
                    {
                        success = false;
                        break;
                    }
                    // add the new region to our local pool, and snap it
                    StateSnapper snap = regionOfReplacement.snapper();
                    snap.snap();
                    allocator.offHeap.acquired(regionOfReplacement.size());
                    allocatingFrom.append(snap);

                    replacement = regionOfReplacement.allocate(copy.limit(), false);
                }
                compact.moves.add(new CompactRegion.Move(copy, replacement, regionOfReplacement));
            }

            if (!success)
            {
                if (!region.transition(OffHeapRegion.State.DISCARDING, OffHeapRegion.State.LIVE))
                    throw new AssertionError();
                if (PoolAllocator.LifeCycle.DISCARDED == allocator.state.lifeCycle)
                {
                    // if we have finished our GC after the allocator has been discarded, we need to collect
                    // the region anyway, so reclaim it for ourselves
                    if (region.transition(OffHeapRegion.State.LIVE, OffHeapRegion.State.DISCARDING))
                    {
                        adjustReclaiming(region);
                        collect.append(region);
                    }
                }
                // reset the regions so we can reuse the memory
                for (StateSnapper newRegion : allocatingFrom)
                    newRegion.reset();
                return false;
            }

            // move past regions we don't expect to be able to allocate from again
            for (StateSnapper newRegion : allocatingFrom)
                if (!newRegion.region.mayContinueAllocating())
                    allocatingFrom.advanceIfHead(newRegion);

            adjustReclaiming(region);
            collect.append(region);
            this.compact.append(compact);

            if (logger.isTraceEnabled())
            {
                logger.trace("Marked {} allocs, with {} bytes for retention from OffHeapAllocator@{}@{}", compact.moves.size(),
                             reclaiming, System.identityHashCode(allocator), allocator.group.name);
            }

            return true;
        }

        void adjustReclaiming(OffHeapRegion region)
        {
            // transfer ownership of the memory to us
            if (!allocator.offHeap.transfer(region.size()))
                throw new AssertionError();
            reclaiming += region.size();
            allocator.offHeap.transferReclaiming(region.size());
        }

        void finishReallocating()
        {
            // and signal the GC to finish
            allocator.transition(PoolAllocator.Gc.COLLECTING);
            // signal the collector thread there's no more work
            compact.append(null);
            // signal any threads waiting on the mark phase
            doneMark.countDown();
        }


        // COMPACTION PHASE

        /**
         * This method does the bulk of the actual collection. Once passed the allocations we're compacting
         * and their target locationsm it deals with copying the data and synchronising the release of the
         * resources that we've freed up.
         */
        @Override
        public void run()
        {
            {   // we're only copying buffers that were *allocated* prior to this starting, however we need to also
                // be certain they've been populated by the writer that allocated them, so we wait for any writers
                // to complete; we do this with awaitSafe(), not await(), because the writers won't block until
                // after completing any write to any buffer it has allocated, and this way we won't deadlock, and don't
                // need to use markBlocking()
                OpOrder.Barrier writeBarrier = allocator.group.writes.newBarrier();
                writeBarrier.issue();
                writeBarrier.awaitSafe();
            }

            long retaining = 0;
            int allocCount = 0;
            CompactRegion compact;

            // TODO: if we allocate two read barriers, one here and one later, we can avoid marking anything that wasn't
            // compacted, but whose read op finished after the first barrier

            // map of markKey -> delay recycle, for all regions we're collecting
            // we use a multi-map so that we can set the mark key of any compacted regions to their new region during the
            // compaction pass. (so new markKey maps to all regions it receives data from), the corollary being we might
            // end up marking something as referenced when it isn't really. this should hopefully be rare, though, and
            // is relatively benign. we attempt to mitigate this issue by only marking those that are
            final Multimap<byte[], OffHeapDelayedRecycle> markLookup = ArrayListMultimap.create(16, 2);

            final OpOrder.Group gcOperation = gcOperations.start();
            try
            {

                while ( null != (compact = this.compact.advanceOrWait()) )
                {
                    OffHeapRegion regionIn = compact.regionIn;
                    markLookup.put(regionIn.markKey, new OffHeapDelayedRecycle(allocator.group.pool, regionIn));

                    OffHeapRegion regionOut = null;
                    for (CompactRegion.Move move : compact.moves)
                    {
                        if (regionOut != move.regionOut)
                            markLookup.putAll((regionOut = move.regionOut).markKey, markLookup.get(regionIn.markKey));

                        // write the data to the new location
                        move.out.duplicate().put(move.in.duplicate());
                        // assign ourselves to the new region by atomically swapping our parent
                        if (!OffHeapRegion.swapParent(move.in, regionIn, move.regionOut))
                        {
                            // if we fail, we've raced with somebody who freed the memory, so
                            // free it from the new region
                            assert OffHeapRegion.getParent(move.in) == null;
                            move.regionOut.forceFree(move.in);
                        }
                        else
                        {
                            OffHeapRegion.lazySetMarkKey(move.in, move.regionOut.markKey);
                            OffHeapRegion.lazySetAddress(move.in, OffHeapRegion.getAddress(move.out));
                            move.regionOut.lazyAdopt(move.in);
                        }
                        retaining += move.in.limit();
                    }
                    allocCount += compact.moves.size();
                }

                if (collect.isEmpty())
                    return;

                // go through all our to-collect regions in case we're collecting ones that are entirely empty,
                // as they won't have been added to the markLookup
                for (OffHeapRegion region : collect)
                    if (!markLookup.containsKey(region.markKey))
                        markLookup.put(region.markKey, new OffHeapDelayedRecycle(allocator.group.pool, region));

                // wait for any outstanding 'reads' to complete. we treat writes as a reads here, as they also need to read consistently
                OpOrder.Barrier readBarrier = allocator.group.reads.newBarrier();
                readBarrier.issue();
                OpOrder.Barrier writeBarrier = allocator.group.writes.newBarrier();
                writeBarrier.issue();
                readBarrier.await();
                // we wait until after all prior reads have completed before visiting the referrers to ensure they are populated
                allocator.group.mark(readBarrier, markLookup);
                // we only awaitSafe() on the write barrier, as we just need ordering guarantees (that they're using the latest addresses).
                writeBarrier.awaitSafe();

                // unmark each OFDR, so that only those marked by a Referrer are left; we want to unmark only once, so check equality with the markKey
                for (Map.Entry<byte[], Collection<OffHeapDelayedRecycle>> entry : markLookup.asMap().entrySet())
                    for (OffHeapDelayedRecycle delayedRecycle : entry.getValue())
                        if (delayedRecycle.region.markKey == entry.getKey())
                            delayedRecycle.unmark();

                int regionCount = removeRegions(collect, allocator.allocated);

                // have the allocator adopt the regions we've migrated its live data to.
                // we only insert these into the allocator's owned collection once we are finished, so that there are
                // no races between our copying/adoption and the cleaner that may be collecting data to start us again
                for (StateSnapper snap : allocatedFrom)
                    allocator.adopt(snap.region);

                // in the unlikely event we finish a GC after the allocator is discarded, we repeat the discard work
                // to free up any regions we just allocated
                if (allocator.state.lifeCycle == PoolAllocator.LifeCycle.DISCARDED)
                    allocator.doDiscard();

                if (logger.isDebugEnabled())
                {
                    logger.debug("Collected {} bytes from {} regions, retaining {} bytes in {} allocs from {}@{}",
                            reclaiming, regionCount, retaining, allocCount, allocator.group.name, System.identityHashCode(allocator));
                }
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException();
            }
            catch (Throwable t)
            {
                logger.error("Fatal exception during GC", t);
            }
            finally
            {
                gcOperation.finishOne();
                running.remove(allocator, this);
                allocator.transition(PoolAllocator.Gc.INACTIVE);
            }
        }

    }

    // just does a little tidying, by removing any no-longer live regions from the allocator
    private static int removeRegions(Iterable<OffHeapRegion> toRemove, NonBlockingQueueView<OffHeapRegion> from)
    {
        IdentityHashSet<OffHeapRegion> lookup = new IdentityHashSet<>();
        for (OffHeapRegion region : toRemove)
            lookup.add(region);
        int count = lookup.size();
        SafeRemoveIterator<OffHeapRegion> removeIter = from.iterator();
        while (removeIter.hasNext())
        {
            if (lookup.remove(removeIter.next()))
            {
                boolean removed = removeIter.safeRemove();
                assert removed;
            }
        }
        assert lookup.isEmpty();
        return count;
    }

}