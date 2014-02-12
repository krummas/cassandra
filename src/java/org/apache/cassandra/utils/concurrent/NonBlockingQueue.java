package org.apache.cassandra.utils.concurrent;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A simple queue API on which all operations are non-blocking and atomic wrt each other (except Iterator removal).
 * This queue is designed with predictable performance and ease of understanding in mind, NOT absolute performance,
 * though it should perform very well still.
 *
 * There are a number of differences to {@link java.util.concurrent.ConcurrentLinkedQueue}:
 * (see below for expansion)
 * <p>1) All methods are wait-free
 * <p>2) CAS-like operations are supported for modifying the head and tail of the queue
 * <p>3) CAS-like removes are supported through SafeRemoveIterator
 * <p>4) fast but unsafe removes are supported through normal Iterator
 * <p>5) the queue may be cheaply snap-shotted, to provide consistent iterations
 * <p>6) multiple consumers of the queue are able to see (view) the same stream of events, through a NonBlockingQueueView,
 * which may also optionally co-ordinate removal of items from the shared stream.
 *
 * To support all of this functionality, there are two distinct semantics this queue supports, simultaneously, that
 * should be understood, namely: advance, and remove. Either of these taken against just one NBQ will behave much
 * like poll() on any normal queue, however once there are multiple views on the queue these two concepts diverge.
 *
 * advance simply moves the current view of the queue forwards, acting like a poll() on that view only, leaving all other
 * queues to consume that element when they reach it.
 * remove(), on the otherhand, effectively flattens all queues into one queue for purposes of that operation,
 * i.e. if all queues were to only use remove() the semantics would be the same as if all were using the same queue,
 * or to put it another way, remove() operations are atomic and visible across all queues, so once removed you can be
 * certain that nobody else also removed the item, nor that anybody else will now see it after.
 * Note that some methods with one kind of semantic may have specific interaction characteristics when interleaved with
 * actions of the opposing semantics. However typically they are all atomic wrt each other.
 *
 * <p>Unsafe iterator removal:
 * To help certain use cases, Iterator removal is supported from the middle of the list, but it is not guaranteed to
 * succeed, and if it is interleaved with competing removes  it may be ignored, and may even restore previously
 * successfully removed items. It will never remove the tail item. It should not be used in situations where removal
 * is anything more than a convenience, or where consistency of removal is required, however it is fast and in
 * tests works correctly >99% of the time.
 *
 * <p>Safe iterator removal:
 * A stronger remove() is provided for cases where safe removal is necessary, for which the SafeRemoveIterator interface
 * exposes the functionality. In this case if you call safeRemove() and true is returned, you can be certain that
 * the item has been removed, will not reappear, and that no other thread has also independently removed it. i.e. it is
 * atomic like all the other operations on the queue. We do not force this stronger remove() for all operations
 * because it is appreciably more expensive. Note that references to the data may or may not be freed up immediately.
 *
 * <p>Snapshots and Views:
 * A view is a mostly read-only copy of the queue from the point it is at when created, and including all items
 * added from them on, supporting all of the same methods as this queue for polling, iterating or removing items.
 * See {@link NonBlockingQueueView}.
 * A snapshot is like a view, but also captures the end at the time of creation, so that no new items are returned.
 * It only supports iteration, and conversion into a View,
 *
 * Note that the queue will always keep a reference to the last item polled.
 *
 * @param <V>
 */
public class NonBlockingQueue<V> extends NonBlockingQueueView<V> implements Iterable<V>
{

    /**
     * The queue is effectively two pointers into a singly linked list which can only safely be appended to;
     * advance() only advances the head pointer, with the prefix left for GC to deal with. As a result we can easily
     * create persistent views and snapshots of the list, which are supported through the snap() and view() methods
     * respectively. Calls to advance?() on a view of the queue do not affect any other view, including the original.
     * Calls to remove(), however, are reflected in this queue and all views on it, and are atomic with respect to all
     * other remove() operations on all other views and this queue.
     *
     */

    /**
     * the tail pointer of the list - this is kept mostly exactly up-to-date (except brief races which cause it to lag)
     * since the list is immutably traversable to the tail, any chain of next pointers will bring you to the real tail
     */
    private volatile Node<V> tail;

    static final AtomicReferenceFieldUpdater<NonBlockingQueue, Node> tailUpdater = AtomicReferenceFieldUpdater.newUpdater(NonBlockingQueue.class, Node.class, "tail");

    public NonBlockingQueue()
    {
        super(new Node<V>(null));
        tail = head;
    }

    /**
     * Add the provided item to the end of the queue
     *
     * @param append
     */
    public void append(V append)
    {
        Node<V> newTail = new Node<>(append);
        while (true)
        {
            Node<V> tail = tailNode();
            tail.initSuccessor(newTail);
            if (Node.nextUpdater.compareAndSet(tail, null, newTail))
                return;
        }
    }

    /**
     * Add <code>append</code> to the end of the queue iff the end of the queue is currently
     * <code>expectedTail</code>. Note that this works even if the queue is now empty but the last item
     * prior to the queue being empty was <code>expectedTail</code>.
     *
     * @param expectedTail the last item we expect to be in the queue, or the last item returned if empty
     * @param append the item to add
     * @return true iff success
     */
    public boolean appendIfTail(V expectedTail, V append)
    {
        Node<V> tail = tailNode();
        if (expectedTail != tail.value)
            return false;
        Node<V> newTail = new Node<>(append);
        tail.initSuccessor(newTail);
        return Node.nextUpdater.compareAndSet(tail, null, newTail);
    }

    /**
     * Advances the queue past the current tail, leaves GC to deal with the mess.
     */
    public void clear()
    {
        head = tailNode();
    }

    /**
     * Returns the tail of the queue. NOTE that this succeeds even if the queue is empty,
     * returning the most recently polled item in this case. If the queue has always been empty, null
     * is returned.
     * @return the tail of the queue
     */
    public V tail()
    {
        return tailNode().value;
    }

    /**
     * Finds the tail and updates the global tail property if the tail we find is not the same
     *
     * @return
     */
    protected final Node<V> tailNode()
    {
        while (true)
        {
            Node<V> prev = tail;
            Node<V> tail = prev;
            while (tail.next != null)
                tail = tail.next;
            // we perform a cas to make sure we never get too far behind the head pointer,
            // to avoid retaining more garbage than necessary
            if (prev == tail || tailUpdater.compareAndSet(this, prev, tail))
                return tail;
        }
    }

    /**
     * advance the queue to the end, placing any items currently present onto the provided list.
     * any remove() operations performed during this operation may be ignored, and the items still
     * appear on the queue, however it is atomic wrt all advance() operations on this queue.
     * It is also very fast.
     *
     * @param list the list to append items to
     * @return the number of items appended to the list
     */
    public int drainTo(List<? super V> list)
    {
        Node<V> cursor, tail;
        while (true)
        {
            cursor = this.head;
            tail = tailNode();
            if (headUpdater.compareAndSet(this, cursor, tail))
                break;
        }
        int items = 0;
        while (cursor != null && cursor.isBefore(tail))
        {
            V next = cursor.value;
            if (!cursor.isDeleted())
            {
                list.add(next);
                items++;
            }
            cursor = cursor.next;
        }
        return items;
    }

    /**
     * @return the size of the queue, overcounted by any items that have been removed from its middle
     */
    public int approxSize()
    {
        Node<V> head = this.head, tail = tailNode();
        return Math.max(0, (int) head.delta(tail));
    }

    /**
     * Returns an Iterable over all items currently in the queue, excluding some subset of those that are deleted
     * using Iterator.remove().
     *
     * @return
     */
    public Snap<V> snap()
    {
        Node<V> head = this.head;
        return new SnapImpl(head, tailNode());
    }

    /**
     * Represents a consistent sub-queue, whose start and end are unaffected by poll() and append() operations on the
     * queue it is formed from, but will reflect any 'successful' Iterator removals, as defined elsewhere.
     * @param <V>
     */
    public static interface Snap<V> extends Iterable<V>
    {
        /**
         * @return the last item in this snap, or null if the snap is empty
         */
        public V tail();

        /**
         * @return a view of the entire queue, starting at the same position as this Snap
         */
        public NonBlockingQueueView<V> view();

        public SafeRemoveIterator<V> iterator();

        /** Extend the tail of this snap to the end of the queue it was constructed from, as it now stands */
        public Snap<V> extend();
    }

    private class SnapImpl implements Snap<V>
    {
        final Node<V> head;
        final Node<V> tail;
        public SnapImpl(Node<V> head, Node<V> tail)
        {
            this.head = head;
            this.tail = tail;
        }

        @Override
        public SafeRemoveIterator<V> iterator()
        {
            return new SliceIterator(head, tail);
        }

        public Snap<V> extend()
        {
            return new SnapImpl(head, tailNode());
        }

        @Override
        public V tail()
        {
            return tail == head ? null : tail.value;
        }

        public NonBlockingQueueView<V> view()
        {
            return new NonBlockingQueueView<>(head);
        }

    }

}
