package org.apache.cassandra.utils.memory;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Multimap;

import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * A collection of allocators that are logically grouped, and which can share OpOrder guarding
 * GC root exploration is done at the group level, which map to a CFS basically, because most
 * operations are contained within one CFS we only need to explore the roots for the CFS we're collecting
 */
public class OffHeapAllocatorGroup extends AllocatorGroup<OffHeapPool>
{

    /** The set of live allocators associated with this group */
    final ConcurrentHashMap<OffHeapAllocator, Boolean> live = new ConcurrentHashMap<>(16, .75f, 1);

    /** Our GC roots */
    final Referrers referrers = new Referrers();

    public OffHeapAllocatorGroup(String name, OffHeapPool parent, OpOrder reads, OpOrder writes)
    {
        super(name, parent, reads, writes);
        parent.groups.append(new SoftReference<>(this));
    }

    public OffHeapAllocator newAllocator()
    {
        OffHeapAllocator allocator = new OffHeapAllocator(this);
        live.put(allocator, Boolean.TRUE);
        return allocator;
    }

    void mark(OpOrder.Barrier readBarrier, Multimap<byte[], OffHeapDelayedRecycle> collecting)
    {
        referrers.mark(readBarrier, collecting);
    }

}
