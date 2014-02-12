package org.apache.cassandra.utils.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

class PoolCleaner<P extends Pool> implements Runnable
{

    private static final Logger logger = LoggerFactory.getLogger(PoolCleaner.class);

    /** The pool we're cleaning */
    final P pool;

    /** should ensure that at least some memory has been marked reclaiming after completion */
    final Runnable clean;

    /** signalled whenever needsCleaning() may return true */
    final WaitQueue wait = new WaitQueue();

    PoolCleaner(P pool, Runnable clean)
    {
        this.pool = pool;
        this.clean = clean;
    }

    boolean needsCleaning()
    {
        return pool.offHeap.needsCleaning() || pool.onHeap.needsCleaning();
    }

    // should ONLY be called when we really think it already needs cleaning
    void trigger()
    {
        wait.signal();
    }

    void clean()
    {
        clean.run();
    }

    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                while (!needsCleaning())
                {
                    final WaitQueue.Signal signal = wait.register();
                    if (!needsCleaning())
                        signal.awaitUninterruptibly();
                    else
                        signal.cancel();
                }
                clean();
            }
        }
        catch (Throwable t)
        {
            logger.error("Fatal PoolCleaner error", t);
        }
    }

    public OpOrder.Barrier getGCBarrier()
    {
        return null;
    }

    // try to do some aggressive cleaning
    void forceClean()
    {

    }

}
