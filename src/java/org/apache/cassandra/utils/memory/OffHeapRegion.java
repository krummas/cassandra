package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.AtomicReferenceArrayUpdater;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A small contiguous portion of memory that is managed by an {@link OffHeapPool}, is assigned to specific a
 * {@link OffHeapAllocator} from which it serves allocation requests.
 *
 * A brief outline of the life-cycle of an allocation, and the region itself:
 *
 * Region is LIVE: we may allocate and free
 *
 * allocate:
 * 1)  allocOffset is bumped atomically by the desired size, if doing so wouldn't overflow the buffer
 *     (otherwise returns failure)
 * 2a) If GC is disabled, we're done, we just return our memory slice
 * 2b) Otherwise we set the markKey and parent of the allocation, and place it in our child collection
 * 3)  We then increment allocSize by the size of the allocation. Note that this is not ordered wrt other
 *     modifications to allocSize (unlike our update to allocOffset). We use it only to provide ordering
 *     guarantees to GC Candidate Selection that children() is fully populated when allocSize == allocOffset.
 *     It also provides the primitive memory ordering (volatile semantics) for this and setting of the markKey/parent.
 *
 * free:
 * 1) We simply atomically swap the parent to null, or fail if already nulled by another;
 * 2) We then bump freeSize by the size of the allocation in the parent we atomically cleared; this is used only
 *    for GC candidate selection to make ordering the regions cheap
 *
 * Region is DISCARDING: We may only free().
 * This state indicates we are either GCing the region, or the owning allocator is discarded.
 * The region will remain DISCARDING until all references to it have been cleared.
 *
 * Region is DISCARDED: The region should no longer be referenced, so no more operations are now permitted.
 *
 * Region is RECYCLED: The region has (or is in the process of) recycling its resources back into the owning pool
 *
 */
final class OffHeapRegion
{

    static final int MAX_SKIP_COUNT = 128; // if we fail this many allocations in a region, we retire it
    static final int MAX_SKIP_SIZE = 1024; // if we fail an allocation in a region with less than this remaining, we retire it
    static final boolean ISGCPOSSIBLE;     // false if we failed to load the unsafe machinery necessary

    /**
     * Point any retired memory to a location that is protected, so that the VM SIGSEGVs if we attempt to access it;
     * this prevents any risk of silent bugs. for now we leave it on by default, at least until after the beta phase.
     */
    private static final boolean PARANOID_RECYCLE = !Objects.equals("off", System.getProperty("cassandra.paranoidgc"));

    // used to co-ordinate between GC and setDiscarded()
    static enum State { LIVE, DISCARDING, DISCARDED, RECYCLED }

    /** Actual underlying data */
    private final ByteBuffer data;

    /** The BBs we have allocated from this region. Only populated if allocSize >= 0 (true isGcEnabled constructor arg) */
    private final Children children;

    private volatile State state = State.LIVE;

    /** Offset for the next allocation */
    private volatile int allocOffset;

    /**
     * Total size of allocations satisfied from this buffer.
     * If !isGcEnabled constructor arg, allocSize will be set to -1, and remain so.
     */
    private volatile int allocSize;

    // TODO: can encode in top bits of allocSize to save space
    /** Total number of allocations we have tried but failed to satisfy with this buffer */
    private volatile int skipCount;

    // TODO: we can overload allocSize if we introduce a new State that we switch when we're finished allocating
    /** Total size of space we have previously allocated and since freed */
    private volatile int freeSize;

    /** This key is assigned to any BB allocated from us, and is used during the final GC phase to count our referrers */
    final byte[] markKey = new byte[0];

    OffHeapRegion(int size, boolean isGcEnabled, boolean limitToOneChild)
    {
        this(ByteBuffer.allocateDirect(size), isGcEnabled, limitToOneChild);
    }

    private OffHeapRegion(OffHeapRegion recycle)
    {
        // we never recycle regions that are limited to only one child, as these are always oversized regions
        this(recycle.data, recycle.allocSize >= 0, false);
    }

    private OffHeapRegion(ByteBuffer data, boolean isGcEnabled, boolean limitToOneChild)
    {
        this.data = data;
        if (!isGcEnabled)
        {
            this.allocSize = -1;
            this.children = null;
        }
        else
        {
            this.children = limitToOneChild ? new Children(1) : new Children();
            this.allocSize = 0;
        }
    }

    /**
     * @param adopt if false the allocated BB will not be added to the child set
     * @return a ByteBuffer of size if there was sufficient room, or null otherwise
     */
    ByteBuffer allocate(int size, boolean adopt)
    {
        while (true)
        {
            int oldOffset = allocOffset;
            if (oldOffset + size > data.capacity())
            {
                // count the failure so we can retire this region if we fail too many times
                skipCountUpdater.incrementAndGet(this);
                return null;
            }

            // Try to atomically claim this region
            if (allocOffsetUpdater.compareAndSet(this, oldOffset, oldOffset + size))
            {
                // we got the alloc
                ByteBuffer dup = data.duplicate();
                dup.position(oldOffset).limit(oldOffset + size);
                if (allocSize < 0)
                    return dup;
                // we slice so that we can GC atomically by setting address
                dup = dup.slice();
                // unsafely unset the parent buffer of dup; this is for three reasons:
                // 1) after calling slice(), parent will be the intermediate duplicate, which only we have a
                //    reference to which is a complete waste of memory;
                // 2) after GC we may have dangling references to parents that are not alive anymore; this
                //    memory leak would be small, but we prefer not to incur it anyway
                // 3) since we've been constructed from a parent, the cleaner will be null anyway, so we don't have
                //    to worry about the memory being collected for us
                // TODO : see about changing behaviour of att in JDK, seems to be a bug as stands
                // We then overload the parent/att property of the ByteBuffer to retain info for us, namely the OffHeapRegion
                // that owns the BB. Without doing this determining the owning region is very expensive.
                lazySetMarkKey(dup, markKey);
                lazySetParent(dup, this);
                if (adopt)
                    children.lazyAdd(dup);
                allocSizeUpdater.addAndGet(this, size);
                return dup;
            }
        }
    }

    /** @return false if we no longer consider the region capable of allocating space */
    boolean mayContinueAllocating()
    {
        return data.capacity() - allocOffset >= MAX_SKIP_SIZE && skipCount <= MAX_SKIP_COUNT;
    }

    /** move our allocOffset and allocSize to the end, and mark any unused space as free */
    void finishAllocating()
    {
        if (allocOffset == data.capacity())
            return;
        int delta = data.capacity() - allocOffsetUpdater.getAndSet(this, data.capacity());
        if (delta != 0)
        {
            freeSizeUpdater.addAndGet(this, delta);
            allocSizeUpdater.addAndGet(this, delta);
        }
    }

    /** Mark the BB unused in its owning OffHeapRegion. This method should return true exactly once for each buffer. */
    static boolean free(ByteBuffer buffer)
    {
        while (true)
        {
            Object obj = getParent(buffer);
            if (obj == null)
                return false;
            OffHeapRegion parent = (OffHeapRegion) obj;
            if (!swapParent(buffer, parent, null))
                continue;
            freeSizeUpdater.addAndGet(parent, buffer.limit());
            return true;
        }
    }

    /** @return true if the buffer is still allocating space */
    boolean isAllocating()
    {
        return allocSize != data.capacity();
    }

    boolean isLive()
    {
        return state == State.LIVE;
    }

    boolean isDiscarded()
    {
        return state.compareTo(State.DISCARDED) >= 0;
    }

    public int size()
    {
        return data.capacity();
    }

    public float freeRatio()
    {
        if (freeSize == size())
            return 1f;
        return freeSize / (float) size();
    }

    public boolean isAllFree()
    {
        return freeSize == size();
    }

    /**
     * Account for the removal of the provided buffer, without checking if the buffer is associated with us.
     * This is used by GC for dealing with races against another free() of the buffer it was copying here.
     */
    void forceFree(ByteBuffer buffer)
    {
        freeSizeUpdater.addAndGet(this, buffer.limit());
    }

    void lazyAdopt(ByteBuffer child)
    {
        children.lazyAdd(child);
    }

    Iterable<ByteBuffer> children()
    {
        return children;
    }

    /** CAS the state, returning success */
    boolean transition(State exp, State upd)
    {
        return state == exp && stateUpdater.compareAndSet(this, exp, upd);
    }

    StateSnapper snapper()
    {
        return new StateSnapper(this);
    }

    void releaseNativeMemory()
    {
        ((DirectBuffer) data).cleaner().clean();
    }

    /**
     * only to be called once the region has been discarded - recycles reusable resources and creates a new region over
     * the same buffer to be returned to the pool for reuse
     */
    OffHeapRegion recycle()
    {
        assert state == State.DISCARDED;
        state = State.RECYCLED;

        // PARANOID GC: if the mark key is the same as ours, the buffer should no longer be referenced,
        // so set its address to 0
        if (PARANOID_RECYCLE)
            for (ByteBuffer child : children)
                if (getMarkKey(child) == markKey)
                    lazySetAddress(child, 0);
        children.recycle();
        return new OffHeapRegion(this);
    }

    /**
     * simple iterate-only collection of buffers we have allocated from this region. Until we move away from BB for
     * allocations we need some cheap method of storing the BBs we have allocated so that when we free them we can
     * copy the ones that are still in use.
     */
    static final class Children implements Iterable<ByteBuffer>
    {

        static final int SIZE = 128;
        static final ConcurrentLinkedQueue<Node> recycled = new ConcurrentLinkedQueue<>();

        private volatile Node head;

        private static final AtomicReferenceFieldUpdater<Children, Node> headUpdater = AtomicReferenceFieldUpdater.newUpdater(Children.class, Node.class, "head");

        static final class Node
        {

            private final ByteBuffer[] values;
            private volatile int insert = 0;
            private volatile Node next;

            Node(int size)
            {
                values = new ByteBuffer[size];
            }
            Node(ByteBuffer[] values)
            {
                this.values = values;
            }

            static final AtomicIntegerFieldUpdater<Node> insertUpdater = AtomicIntegerFieldUpdater.newUpdater(Node.class, "insert");
            static final AtomicReferenceArrayUpdater<ByteBuffer> valuesArrayUpdater = new AtomicReferenceArrayUpdater<>(ByteBuffer[].class);

            boolean lazyAdd(ByteBuffer value)
            {
                while (true)
                {
                    int cur = insert;
                    if (cur == values.length)
                        return false;
                    if (insertUpdater.compareAndSet(this, cur, cur + 1))
                    {
                        assert values[cur] == null;
                        valuesArrayUpdater.putOrdered(values, cur, value);
                        return true;
                    }
                }
            }

            Node recycle()
            {
                for (int i = 0 ; i < insert ; i++)
                    valuesArrayUpdater.putOrdered(values, i, null);
                return new Node(values);
            }

        }

        Children()
        {
            this(SIZE);
        }

        Children(int initialSize)
        {
            Node newHead;
            if (initialSize == SIZE)
            {
                newHead = recycled.poll();
                if (newHead == null)
                    newHead = new Node(SIZE);
            }
            else
                newHead = new Node(initialSize);
            head = newHead;
        }

        void lazyAdd(ByteBuffer add)
        {
            while (true)
            {
                Node currentHead = this.head;
                if (currentHead.lazyAdd(add))
                    return;
                Node newHead = recycled.poll();
                if (newHead == null)
                    newHead = new Node(SIZE);
                newHead.next = currentHead;
                if (!headUpdater.compareAndSet(this, currentHead, newHead))
                {
                    // if we fail, put the new head node in the recycled queue so it can be reused
                    // we null out just to make certain we don't keep dangling pointers to something collectible
                    newHead.next = null;
                    recycled.add(newHead);
                }
            }
        }

        void recycle()
        {
            Node current = this.head;
            while (current != null)
            {
                recycled.add(current.recycle());
                Node prev = current;
                current = current.next;
                prev.next = null;
            }
        }

        /**
         * @return an Iterator over all items inserted into this Child collection that are still owned by our parent
         */
        public Iterator<ByteBuffer> iterator()
        {
            return new ChildIterator(head);
        }

        private class ChildIterator implements Iterator<ByteBuffer>
        {

            private Node cursor;
            private int position = 0;
            public ChildIterator(Node cursor)
            {
                this.cursor = cursor;
            }

            @Override
            public boolean hasNext()
            {
                return position != cursor.insert || cursor.next != null;
            }

            @Override
            public ByteBuffer next()
            {
                if (position == cursor.insert)
                {
                    cursor = cursor.next;
                    position = 0;
                    assert cursor != null;
                }
                return cursor.values[position++];
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        }

    }

    /**
     * Wraps an OffHeapRegion, snapping the state of the new local allocation region, so that if we fail to allocate
     * complete room to collect a candidate region, we can reset the new region before placing at the end of the allocator
     * queue we are collecting
     */
    static final class StateSnapper
    {
        final OffHeapRegion region;
        int allocOffset;
        int allocSize;
        int skipCount;
        int freeSize;

        private StateSnapper(OffHeapRegion region)
        {
            this.region = region;
        }

        void snap()
        {
            allocOffset = region.allocOffset;
            allocSize = region.allocSize;
            freeSize = region.freeSize;
            skipCount = region.skipCount;
        }

        void reset()
        {
            region.allocOffset = allocOffset;
            region.allocSize = allocSize;
            region.freeSize = freeSize;
            region.skipCount = skipCount;
        }
    }

    private static final AtomicIntegerFieldUpdater<OffHeapRegion> allocOffsetUpdater = AtomicIntegerFieldUpdater.newUpdater(OffHeapRegion.class, "allocOffset");
    private static final AtomicIntegerFieldUpdater<OffHeapRegion> allocSizeUpdater = AtomicIntegerFieldUpdater.newUpdater(OffHeapRegion.class, "allocSize");
    private static final AtomicIntegerFieldUpdater<OffHeapRegion> skipCountUpdater = AtomicIntegerFieldUpdater.newUpdater(OffHeapRegion.class, "skipCount");
    private static final AtomicIntegerFieldUpdater<OffHeapRegion> freeSizeUpdater = AtomicIntegerFieldUpdater.newUpdater(OffHeapRegion.class, "freeSize");
    private static final AtomicReferenceFieldUpdater<OffHeapRegion, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(OffHeapRegion.class, State.class, "state");

    private static final Unsafe unsafe;
    private static final long bufferAddressOffset;
    private static final long bufferAttOffset;
    private static final long hbOffset;
    static
    {
        Unsafe _unsafe = null;
        long _bufferAddressOffset = 0, _bufferAttOffset = 0, _hbOffset = 0;
        boolean success = false;
        try
        {
            final ByteBuffer bb = ByteBuffer.allocateDirect(0);
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            _unsafe = (sun.misc.Unsafe) field.get(null);
            _bufferAddressOffset = _unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            _bufferAttOffset = _unsafe.objectFieldOffset(bb.getClass().getDeclaredField("att"));
            _hbOffset = _unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
            success = true;
        }
        catch (Exception e)
        {
        }
        ISGCPOSSIBLE = success;
        unsafe = _unsafe;
        bufferAddressOffset = _bufferAddressOffset;
        bufferAttOffset = _bufferAttOffset;
        hbOffset = _hbOffset;
    }

    static long getAddress(Buffer buffer)
    {
        return unsafe.getLong(buffer, bufferAddressOffset);
    }

    private static void lazySetParent(Buffer buffer, OffHeapRegion owner)
    {
        unsafe.putOrderedObject(buffer, bufferAttOffset, owner);
    }

    static void lazySetAddress(Buffer buffer, long address)
    {
        unsafe.putOrderedLong(buffer, bufferAddressOffset, address);
    }

    static boolean swapParent(ByteBuffer buffer, OffHeapRegion exp, Object upd)
    {
        return unsafe.compareAndSwapObject(buffer, bufferAttOffset, exp, upd);
    }

    static Object getParent(ByteBuffer buffer)
    {
        return unsafe.getObjectVolatile(buffer, bufferAttOffset);
    }

    static byte[] getMarkKey(ByteBuffer buffer)
    {
        return (byte[]) unsafe.getObject(buffer, hbOffset);
    }

    static void lazySetMarkKey(ByteBuffer buffer, byte[] markKey)
    {
        unsafe.putOrderedObject(buffer, hbOffset, markKey);
    }

}