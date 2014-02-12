package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Represents an action to take on any unsafe memory we access during a transaction to make it safe
 * outside of the transaction. There are three such actions:
 * Unsafe:
 *   Do nothing. It will be unsafe unless otherwise protected.
 * OnHeap:
 *   Copy the memory onto the heap so it is managed by the JVM. This is useful in situations where
 *   the code complexity for ensuring safety of access would be too great, or too error prone.
 * Refer:
 *   Register the memory with one of our managed GC roots, until setDone() is called on it (which
 *   indicates we no longer need the reference)
 */
public abstract class RefAction
{

    static final class OnHeap extends RefAction
    {
        static final OnHeap INSTANCE = new OnHeap();

        public RefAction subAction()
        {
            return this;
        }

        public AbstractAllocator allocator()
        {
            return HeapAllocator.instance;
        }
    }

    static final class Unsafe extends RefAction
    {
        static final Unsafe INSTANCE = new Unsafe();

        public RefAction subAction()
        {
            return this;
        }

        public AbstractAllocator allocator()
        {
            return null;
        }
    }

    public static RefAction allocateOnHeap()
    {
        return OnHeap.INSTANCE;
    }

    public static RefAction unsafe()
    {
        return Unsafe.INSTANCE;
    }

    public static Referrer refer()
    {
        return new Referrer();
    }

    /**
     * Complete the action on the given value, retrieved from the provided group, during the readOp transaction.
     * In general this is a no-op, except for the Refer action, which registers it with a GC root.
     *
     * @param group group val came from
     * @param readOp read was guarded by this txn
     * @param val the value we retrieved
     */
    public void complete(AllocatorGroup group, OpOrder.Group readOp, Object val)
    {

    }

    /**
     * Indicate the memory guarded by this RefAction is no longer need (by the action that started the RefAction)
     */
    public void setDone()
    {

    }

    /**
     * @return the action to take on any sub-operation that will ultimately be guarded by this RefAction.
     */
    public abstract RefAction subAction();

    /**
     * @return the allocator to copy any memory to, if any
     */
    public abstract AbstractAllocator allocator();

}
