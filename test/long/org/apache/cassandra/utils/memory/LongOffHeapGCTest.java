package org.apache.cassandra.utils.memory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.mina.util.IdentityHashSet;
import org.junit.*;
import org.slf4j.*;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LongOffHeapGCTest
{

    static long RUNTIME = TimeUnit.MINUTES.toMillis(5);
    static final long REPORT_INTERVAL = TimeUnit.SECONDS.toMillis(1);

    static
    {
        System.setProperty("cassandra.paranoidgc", "on");
    }

    private static final Logger logger = LoggerFactory.getLogger(LongOffHeapGCTest.class);

    private static final class BufferCounter
    {
        final AtomicLong shared;
        final int group;
        int buffer = 0;
        private BufferCounter(AtomicLong shared, int group)
        {
            this.shared = shared;
            this.group = group;
        }
        void increment()
        {
            add(1);
        }
        void add(int amount)
        {
            buffer += amount;
            if (buffer >= group)
            {
                shared.addAndGet(buffer);
                buffer = 0;
            }
        }
        void flush()
        {
            shared.addAndGet(buffer);
            buffer = 0;
        }
    }

    private static final class Stats
    {
        final AtomicLong totalCount = new AtomicLong();
        final AtomicLong keepCount = new AtomicLong();
        final AtomicLong checkCount = new AtomicLong();
        final AtomicLong refCheckCount = new AtomicLong();
        final AtomicLong totalBytes = new AtomicLong();
        final AtomicLong uniqueErrors = new AtomicLong();
        final AtomicLong repeatErrors = new AtomicLong();
        final StatSnap start = snap();
        StatSnap last = start;
        StatSnap snap()
        {
            return new StatSnap(System.currentTimeMillis(), totalCount.get(), keepCount.get(), checkCount.get(),
                    refCheckCount.get(), totalBytes.get(), uniqueErrors.get(), repeatErrors.get(), uniqueErrors.get());
        }
        void log()
        {
            StatSnap cur = snap();
            logDelta(last, cur);
            last = cur;
        }
        void logFinal()
        {
            logDelta(start, last);
        }
        void logDelta(StatSnap prev, StatSnap cur)
        {
            StatSnap delta = cur.delta(prev);
            logger.info(String.format("%.0f%% elapsed: %dMb, %d clones, %d kept, %d checks, %d refchecks, %d errors, %d repeat errors, %d total errors",
                    100 * (cur.time - start.time) / (float) RUNTIME, delta.totalBytes >> 20,
                    delta.totalCount, delta.keepCount, delta.checkCount, delta.refCheckCount, delta.uniqueErrors, delta.repeatErrors, delta.totalUniqueErrors
            ));
            float seconds = delta.time / 1000f;
            logger.info(String.format("%.0fs elapsed: %.0fMb/s, %.0f clones/s, %.0f kept/s, %.0f checks/s, %.0f refchecks/s",
                    seconds, (delta.totalBytes >> 20) / seconds, delta.totalCount / seconds, delta.keepCount / seconds,
                    delta.checkCount / seconds, delta.refCheckCount / seconds
            ));
        }
    }

    private static final class BufferStats
    {
        final BufferCounter totalCount;
        final BufferCounter keepCount;
        final BufferCounter checkCount;
        final BufferCounter refCheckCount;
        final BufferCounter totalBytes;
        final AtomicLong uniqueErrors;
        final AtomicLong repeatErrors;
        BufferStats(Stats stats)
        {
            totalCount = new BufferCounter(stats.totalCount, 64);
            keepCount = new BufferCounter(stats.keepCount, 64);
            checkCount = new BufferCounter(stats.checkCount, 64);
            refCheckCount = new BufferCounter(stats.refCheckCount, 64);
            totalBytes = new BufferCounter(stats.totalBytes, 1 << 16);
            uniqueErrors = stats.uniqueErrors;
            repeatErrors = stats.repeatErrors;
        }
        void flush()
        {
            totalCount.flush();
            keepCount.flush();
            checkCount.flush();
            refCheckCount.flush();
            totalBytes.flush();
        }
    }

    private static final class StatSnap
    {
        final long time;
        final long totalCount;
        final long keepCount;
        final long checkCount;
        final long refCheckCount;
        final long totalBytes;
        final long uniqueErrors;
        final long repeatErrors;
        final long totalUniqueErrors;
        private StatSnap(long time, long totalCount, long keepCount, long checkCount, long refCheckCount, long totalBytes, long uniqueErrors, long repeatErrors, long totalUniqueErrors)
        {
            this.time = time;
            this.totalCount = totalCount;
            this.keepCount = keepCount;
            this.checkCount = checkCount;
            this.refCheckCount = refCheckCount;
            this.uniqueErrors = uniqueErrors;
            this.repeatErrors = repeatErrors;
            this.totalBytes = totalBytes;
            this.totalUniqueErrors = totalUniqueErrors;
        }
        StatSnap delta(StatSnap prev)
        {
            return new StatSnap(this.time - prev.time, this.totalCount - prev.totalCount, this.keepCount - prev.keepCount,
                    this.checkCount - prev.checkCount, this.refCheckCount - prev.refCheckCount, this.totalBytes - prev.totalBytes,
                    this.uniqueErrors - prev.uniqueErrors, this.repeatErrors - prev.repeatErrors, totalUniqueErrors);
        }
    }

    private static final class Referred
    {
        final Referrer referrer;
        final ByteBuffer buffer;
        final ByteBuffer success;
        private Referred(Referrer referrer, ByteBuffer buffer, ByteBuffer success)
        {
            this.referrer = referrer;
            this.buffer = buffer;
            this.success = success;
        }
    }

    @Test
    public void testGC() throws ExecutionException, InterruptedException
    {
        final int writerCount = 8;
        final int readerCount = 1;
        final ExecutorService writers = Executors.newFixedThreadPool(writerCount, new NamedThreadFactory("Writer"));
        final ExecutorService readers = Executors.newFixedThreadPool(readerCount, new NamedThreadFactory("Reader"));
        final NonBlockingQueue<ByteBuffer>[] writing = new NonBlockingQueue[writerCount];
        final NonBlockingQueueView<ByteBuffer>[][] lagged = new NonBlockingQueueView[readerCount][writerCount];

        final int regionSize = 1 << 20;
        final int maxMemory = regionSize * 400;
        final int regions = maxMemory / regionSize;
        final float referRatio = 0.0f;
        final float allocRatio = 0.1f;
        final float cleanThreshold = 0.1f;
        final OpOrder reads = new OpOrder();
        final OpOrder writes = new OpOrder();
        final OffHeapPool pool = new OffHeapPool(regionSize, 1, maxMemory, cleanThreshold, new Runnable() { public void run() { }});
        final OffHeapAllocatorGroup group = pool.newAllocatorGroup("test", reads, writes);
        pool.setGcEnabled(true);

        final int testBufferSize = 128;
        final ByteBuffer[] successes = new ByteBuffer[writerCount];
        final ByteBuffer fail = ByteBuffer.allocate(testBufferSize);

        final Stats stats = new Stats();
        final long start = stats.start.time;
        final long end = start + RUNTIME;
        for (int i = 0 ; i < writerCount; i++)
        {
            successes[i] = ByteBuffer.allocate(testBufferSize);
            ThreadLocalRandom.current().nextBytes(successes[i].array());
            writing[i] = new NonBlockingQueue<>();
            for (int j = 0 ; j < readerCount ; j++)
                lagged[j][i] = writing[i].view();
            final NonBlockingQueue<ByteBuffer> mywriting = writing[i];
            final ByteBuffer success = successes[i];
            writers.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    final BufferStats bstats = new BufferStats(stats);
                    final OffHeapAllocator allocator = group.newAllocator();
                    final int maxCheckingSize = (int) ((maxMemory / writerCount) * allocRatio);
                    int checkingSize = 0;

                    final OpOrder.SyncingGroup write = writes.startSync();
                    while (System.currentTimeMillis() < end)
                    {
                        {
                            ByteBuffer clone = allocator.allocate(testBufferSize, write.current());
                            clone.duplicate().put(success.duplicate());

                            if (checkingSize >= maxCheckingSize)
                                allocator.free(mywriting.advance());
                            else
                                checkingSize += testBufferSize;
                            mywriting.append(clone);
                            bstats.keepCount.increment();
                        }

                        for (int i = testBufferSize ; i < pool.regionSize ; i += testBufferSize)
                        {
                            ByteBuffer clone = allocator.allocate(testBufferSize, write.current());
                            clone.duplicate().put(fail.duplicate());
                            allocator.free(clone);
                        }
                        bstats.totalBytes.add(pool.regionSize);
                        bstats.totalCount.add(pool.regionSize / testBufferSize);
                        write.sync();
                    }
                    write.finish();
                    bstats.flush();
                }
            });
        }

        for (int i = 0 ; i < readerCount; i++)
        {
            final NonBlockingQueueView<ByteBuffer>[] mylagged = lagged[i];
            readers.execute(new Runnable()
            {
                final IdentityHashSet<ByteBuffer> uniqueErrors = new IdentityHashSet<>();
                final BufferStats bstats = new BufferStats(stats);
                @Override
                public void run()
                {
                    final ThreadLocalRandom random = ThreadLocalRandom.current();
                    final IdentityHashMap<ByteBuffer, Referred> referred = new IdentityHashMap<>();
                    final IdentityHashSet<ByteBuffer> seen = new IdentityHashSet<>();
                    final TreeMap<Float, Referred> oldReferred = new TreeMap<>();
                    while (System.currentTimeMillis() < end)
                    {
                        final int referRegions = (int) (regions * referRatio);
                        // check with simple protection within a transaction
                        for (int i = 0 ; i < writing.length ; i++)
                        {
                            final OpOrder.Group read = reads.start();
                            ByteBuffer success = successes[i];
                            NonBlockingQueue<ByteBuffer> check = writing[i];
                            int count = 0;
                            for (ByteBuffer buffer : check)
                            {
                                // each new item we see, reference it with probability referRatio
                                if (seen.add(buffer) && random.nextFloat() < referRatio)
                                {
                                    Referrer referrer = RefAction.refer();
                                    referrer.complete(group, read, buffer);
                                    referred.put(buffer, new Referred(referrer, buffer, success));
                                }
                                check(buffer, success, true);
                                count++;
                            }
                            read.finishOne();
                            bstats.checkCount.add(count);

                            // catch up our view with the original
                            NonBlockingQueueView<ByteBuffer> lagged = mylagged[i];
                            while (lagged.peek() != check.peek())
                            {
                                ByteBuffer freed = lagged.advance();
                                // free up freed from our seen set, as we won't see it again
                                seen.remove(freed);
                                Referred ref = referred.remove(freed);
                                if (ref == null)
                                {
                                    uniqueErrors.remove(freed);
                                }
                                else
                                {
                                    // if we referenced it, put it in our set of freed referred items to check periodically
                                    oldReferred.put(random.nextFloat(), ref);
                                    if (oldReferred.size() > referRegions)
                                    {
                                        // if we have more than referRatio items already, select an item at random to be removed
                                        Float find = random.nextFloat();
                                        Float rem = oldReferred.floorKey(find);
                                        if (rem == null)
                                            rem = oldReferred.ceilingKey(find);
                                        Referred removed = oldReferred.remove(rem);
                                        removed.referrer.setDone();
                                        uniqueErrors.remove(removed.buffer);
                                    }
                                }
                            }
                        }

                        // test the referred buffers
                        for (Referred ref : oldReferred.values())
                        {
                            check(ref.buffer, ref.success, false);
                        }
                        bstats.refCheckCount.add(oldReferred.size());
                    }
                    bstats.flush();
                    for (Referred ref : oldReferred.values())
                        ref.referrer.setDone();
                }

                private void check(ByteBuffer buffer, ByteBuffer success, boolean verbose)
                {
                    if (!buffer.equals(success))
                    {
                        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
                        copy.duplicate().put(buffer.duplicate());
                        if (uniqueErrors.add(buffer))
                            bstats.uniqueErrors.incrementAndGet();
                        else
                            bstats.repeatErrors.incrementAndGet();
                    }
                }
            });
        }

        long nextReport = start + REPORT_INTERVAL;
        while (true)
        {
            long now = System.currentTimeMillis();
            if (now > nextReport)
            {
                stats.log();
                logger.info(String.format("%dKb allocated, %dKb reclaiming of %dKb limit", pool.offHeap.allocated() >> 10, pool.offHeap.reclaiming() >> 10, pool.offHeap.limit >> 10));
                nextReport += REPORT_INTERVAL;
                if (now > end)
                    break;
            }
            long wait = nextReport - System.currentTimeMillis();
            if (wait > 0)
                Thread.sleep(wait);
        }
        readers.shutdown();
        writers.shutdown();
        Assert.assertTrue(readers.awaitTermination(10L, TimeUnit.SECONDS));
        Assert.assertTrue(writers.awaitTermination(10L, TimeUnit.SECONDS));
        Assert.assertEquals("Error count", 0, stats.uniqueErrors.get());
        stats.logFinal();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        RUNTIME = TimeUnit.MINUTES.toMillis(Integer.parseInt(args[0]));
        new LongOffHeapGCTest().testGC();
    }

}
