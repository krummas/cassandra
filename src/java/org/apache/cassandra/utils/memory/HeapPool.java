package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapPool extends Pool
{
    public HeapPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public AllocatorGroup newAllocatorGroup(String name, OpOrder reads, OpOrder writes)
    {
        return new AllocatorGroup<HeapPool>(name, this, reads, writes)
        {

            public PoolAllocator newAllocator()
            {
                return new HeapPoolAllocator(this);
            }
        };
    }
}
