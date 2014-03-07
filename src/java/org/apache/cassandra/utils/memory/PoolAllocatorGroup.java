package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class PoolAllocatorGroup<P extends Pool>
{

    public final String name;
    public final P pool;
    public final OpOrder writes;

    public PoolAllocatorGroup(String name, P pool, OpOrder writes)
    {
        this.name = name;
        this.pool = pool;
        this.writes = writes;
    }

    public abstract PoolAllocator newAllocator();

}
