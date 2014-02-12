package org.apache.cassandra.utils.concurrent;


import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class AtomicReferenceArrayUpdater<E>
{

    // it's quite likely these could all be static. however since the object itself should be static,
    // we hope the VM will inline the methods and values as constant anyway (though this may be optimistic)
    private final long offset;
    private final int shift;

    public AtomicReferenceArrayUpdater(Class<E[]> arrayType)
    {
        offset = theUnsafe.arrayBaseOffset(arrayType);
        shift = shift(theUnsafe.arrayIndexScale(arrayType));

    }

    public final boolean compareAndSet(Object trg, int i, E exp, E upd) {
        return theUnsafe.compareAndSwapObject(trg, offset + (i << shift), exp, upd);
    }

    public final void putVolatile(Object trg, int i, E val) {
        theUnsafe.putObjectVolatile(trg, offset + (i << shift), val);
    }

    public final void putOrdered(Object trg, int i, E val) {
        theUnsafe.putOrderedObject(trg, offset + (i << shift), val);
    }

    public final Object get(Object trg, int i) {
        return theUnsafe.getObject(trg, offset + (i << shift));
    }

    public final Object getVolatile(Object trg, int i) {
        return theUnsafe.getObjectVolatile(trg, offset + (i << shift));
    }

    static final Unsafe theUnsafe;

    private static int shift(int scale)
    {
        if (Integer.bitCount(scale) != 1)
            throw new IllegalStateException();
        return Integer.bitCount(scale - 1);
    }

    static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(
              new PrivilegedAction<Object>()
              {
                  @Override
                  public Object run()
                  {
                      try
                      {
                          Field f = Unsafe.class.getDeclaredField("theUnsafe");
                          f.setAccessible(true);
                          return f.get(null);
                      } catch (NoSuchFieldException e)
                      {
                          // It doesn't matter what we throw;
                          // it's swallowed in getBestComparer().
                          throw new Error();
                      } catch (IllegalAccessException e)
                      {
                          throw new Error();
                      }
                  }
              });
    }

}