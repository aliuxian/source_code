/*
 * Copyright (c) 1997, 2013, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

package java.lang;
import java.lang.ref.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * This class provides thread-local variables.  These variables differ from
 * their normal counterparts in that each thread that accesses one (via its
 * {@code get} or {@code set} method) has its own, independently initialized
 * copy of the variable.  {@code ThreadLocal} instances are typically private
 * static fields in classes that wish to associate state with a thread (e.g.,
 * a user ID or Transaction ID).
 *
 * <p>For example, the class below generates unique identifiers local to each
 * thread.
 * A thread's id is assigned the first time it invokes {@code ThreadId.get()}
 * and remains unchanged on subsequent calls.
 * <pre>
 * import java.util.concurrent.atomic.AtomicInteger;
 *
 * public class ThreadId {
 *     // Atomic integer containing the next thread ID to be assigned
 *     private static final AtomicInteger nextId = new AtomicInteger(0);
 *
 *     // Thread local variable containing each thread's ID
 *     private static final ThreadLocal&lt;Integer&gt; threadId =
 *         new ThreadLocal&lt;Integer&gt;() {
 *             &#64;Override protected Integer initialValue() {
 *                 return nextId.getAndIncrement();
 *         }
 *     };
 *
 *     // Returns the current thread's unique ID, assigning it if necessary
 *     public static int get() {
 *         return threadId.get();
 *     }
 * }
 * </pre>
 * <p>Each thread holds an implicit reference to its copy of a thread-local
 * variable as long as the thread is alive and the {@code ThreadLocal}
 * instance is accessible; after a thread goes away, all of its copies of
 * thread-local instances are subject to garbage collection (unless other
 * references to these copies exist).
 *
 * @author  Josh Bloch and Doug Lea
 * @since   1.2
 */
public class ThreadLocal<T> {
    /**
     * ThreadLocals rely on per-thread linear-probe hash maps attached
     * to each thread (Thread.threadLocals and
     * inheritableThreadLocals).  The ThreadLocal objects act as keys,
     * searched via threadLocalHashCode.  This is a custom hash code
     * (useful only within ThreadLocalMaps) that eliminates collisions
     * in the common case where consecutively constructed ThreadLocals
     * are used by the same threads, while remaining well-behaved in
     * less common cases.
     *
     * 线程获取threadLocal.get()的时候，如果是第一次调用该ThreadLocal的get方法，那么就会给当前线程分配一个value，
     * 这个value和当前的ThreadLocal对象就会被包装成为一个Entry对象，其中key是ThreadLocal对象，value是ThreadLocal为当前线程生成的value，
     * 这个Entry对象会存放在当前线程的threadLocals这个map中，
     *
     * Entry  存放的位置就是   threadLocalHashCode & (table.length - 1)
     */
    private final int threadLocalHashCode = nextHashCode();

    /**
     * The next hash code to be given out. Updated atomically. Starts at
     * zero.
     * 创建ThreadLocal对象时，会使用到的，每创建一个threadLocal对象，就会利用nextHashCode给该对象分配一个值
     */
    private static AtomicInteger nextHashCode =
        new AtomicInteger();

    /**
     * The difference between successively generated hash codes - turns
     * implicit sequential thread-local IDs into near-optimally spread
     * multiplicative hash values for power-of-two-sized tables.
     * 每创建一个threadLocal对象，nextHashCode的值就会增长0x61c88647
     *
     * 斐波拉契数，黄金分割数，hash增量为这个数字，带来的好处就是hash分布非常均匀
     */
    private static final int HASH_INCREMENT = 0x61c88647;

    /**
     * Returns the next hash code.
     * 创建新的ThreadLocal对象时，会使用这个方法给该对象分配一个hash值
     */
    private static int nextHashCode() {
        return nextHashCode.getAndAdd(HASH_INCREMENT);
    }

    /**
     * Returns the current thread's "initial value" for this
     * thread-local variable.  This method will be invoked the first
     * time a thread accesses the variable with the {@link #get}
     * method, unless the thread previously invoked the {@link #set}
     * method, in which case the {@code initialValue} method will not
     * be invoked for the thread.  Normally, this method is invoked at
     * most once per thread, but it may be invoked again in case of
     * subsequent invocations of {@link #remove} followed by {@link #get}.
     *
     * <p>This implementation simply returns {@code null}; if the
     * programmer desires thread-local variables to have an initial
     * value other than {@code null}, {@code ThreadLocal} must be
     * subclassed, and this method overridden.  Typically, an
     * anonymous inner class will be used.
     *
     * @return the initial value for this thread-local
     *
     *
     * 默认返回null，一般都需要重写该方法
     */
    protected T initialValue() {
        return null;
    }

    /**
     * Creates a thread local variable. The initial value of the variable is
     * determined by invoking the {@code get} method on the {@code Supplier}.
     *
     * @param <S> the type of the thread local's value
     * @param supplier the supplier to be used to determine the initial value
     * @return a new thread local variable
     * @throws NullPointerException if the specified supplier is null
     * @since 1.8
     */
    public static <S> ThreadLocal<S> withInitial(Supplier<? extends S> supplier) {
        return new SuppliedThreadLocal<>(supplier);
    }

    /**
     * Creates a thread local variable.
     * @see #withInitial(java.util.function.Supplier)
     */
    public ThreadLocal() {
    }

    /**
     * Returns the value in the current thread's copy of this
     * thread-local variable.  If the variable has no value for the
     * current thread, it is first initialized to the value returned
     * by an invocation of the {@link #initialValue} method.
     *
     * @return the current thread's value of this thread-local
     *
     *
     * 返回当前线程与当前这个ThreadLocal对象相关联的线程局部变量
     * 如果是第一次去调用，那么会进行分配（initialValue()）
     */
    public T get() {
        // 获取当前线程
        Thread t = Thread.currentThread();
        // 获取当前线程的threadLocals变量
        ThreadLocalMap map = getMap(t);
        if (map != null) {
            // 条件成立说明当前线程已经拥有了自己的ThreadLocalMap类型的变量了，

            // this就是当前这个ThreadLocal对象
            // 获取该ThreadLocal关联的Entry
            ThreadLocalMap.Entry e = map.getEntry(this);
            if (e != null) {
                // 线程初始化过与当前ThreadLocal相关联的线程局部变量
                // 所以可以直接返回
                @SuppressWarnings("unchecked")
                T result = (T)e.value;
                return result;
            }
        }
        // 要么map没初始化，要么Entry是null
        /**
         * 1. map不为null 就为当前线程创建与当前ThreadLocal相关联的value，并返回
         * 2. map为null   初始化当前线程的threadLocals变量，并将当前ThreadLocal添加进去
         *
         * 返回value
         */
        return setInitialValue();
    }

    /**
     * Variant of set() to establish initialValue. Used instead
     * of set() in case user has overridden the set() method.
     *
     * @return the initial value
     */
    private T setInitialValue() {
        // 为当前线程创建一个与当前ThreadLocal相关联的局部变量
        T value = initialValue();

        Thread t = Thread.currentThread();
        ThreadLocalMap map = getMap(t);

        // map不为null，那么将当前ThreadLocal添加到map中
        if (map != null)
            map.set(this, value);
        else
            // map还没初始化，进行map的初始化
            createMap(t, value);
        // 返回当前ThreadLocal对象为当前线程创建的局部变量的值
        return value;
    }

    /**
     * Sets the current thread's copy of this thread-local variable
     * to the specified value.  Most subclasses will have no need to
     * override this method, relying solely on the {@link #initialValue}
     * method to set the values of thread-locals.
     *
     * @param value the value to be stored in the current thread's copy of
     *        this thread-local.
     *
     * 修改当前线程与当前ThreadLocal对象相关联的线程局部变量
     */
    public void set(T value) {
        Thread t = Thread.currentThread();
        ThreadLocalMap map = getMap(t);

        /**
         * 两种情况：
         * map不为null，就set  修改 or 添加
         * map为null， 先初始化map，再将ThreadLocal和为当前线程创建的局部变量添加进去
         */
        if (map != null)
            map.set(this, value);
        else
            createMap(t, value);
    }

    /**
     * Removes the current thread's value for this thread-local
     * variable.  If this thread-local variable is subsequently
     * {@linkplain #get read} by the current thread, its value will be
     * reinitialized by invoking its {@link #initialValue} method,
     * unless its value is {@linkplain #set set} by the current thread
     * in the interim.  This may result in multiple invocations of the
     * {@code initialValue} method in the current thread.
     *
     * @since 1.5
     */
     public void remove() {
         ThreadLocalMap m = getMap(Thread.currentThread());
         // map 不为null，就移除
         // map 为null，直接结束了，没啥可以remove的
         if (m != null)
             m.remove(this);
     }

    /**
     * Get the map associated with a ThreadLocal. Overridden in
     * InheritableThreadLocal.
     *
     * @param  t the current thread
     * @return the map
     */
    ThreadLocalMap getMap(Thread t) {
        return t.threadLocals;
    }

    /**
     * Create the map associated with a ThreadLocal. Overridden in
     * InheritableThreadLocal.
     *
     * @param t the current thread
     * @param firstValue value for the initial entry of the map
     */
    void createMap(Thread t, T firstValue) {
        t.threadLocals = new ThreadLocalMap(this, firstValue);
    }

    /**
     * Factory method to create map of inherited thread locals.
     * Designed to be called only from Thread constructor.
     *
     * @param  parentMap the map associated with parent thread
     * @return a map containing the parent's inheritable bindings
     */
    static ThreadLocalMap createInheritedMap(ThreadLocalMap parentMap) {
        return new ThreadLocalMap(parentMap);
    }

    /**
     * Method childValue is visibly defined in subclass
     * InheritableThreadLocal, but is internally defined here for the
     * sake of providing createInheritedMap factory method without
     * needing to subclass the map class in InheritableThreadLocal.
     * This technique is preferable to the alternative of embedding
     * instanceof tests in methods.
     */
    T childValue(T parentValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * An extension of ThreadLocal that obtains its initial value from
     * the specified {@code Supplier}.
     */
    static final class SuppliedThreadLocal<T> extends ThreadLocal<T> {

        private final Supplier<? extends T> supplier;

        SuppliedThreadLocal(Supplier<? extends T> supplier) {
            this.supplier = Objects.requireNonNull(supplier);
        }

        @Override
        protected T initialValue() {
            return supplier.get();
        }
    }

    /**
     * ThreadLocalMap is a customized hash map suitable only for
     * maintaining thread local values. No operations are exported
     * outside of the ThreadLocal class. The class is package private to
     * allow declaration of fields in class Thread.  To help deal with
     * very large and long-lived usages, the hash table entries use
     * WeakReferences for keys. However, since reference queues are not
     * used, stale entries are guaranteed to be removed only when
     * the table starts running out of space.
     */
    static class ThreadLocalMap {

        /**
         * The entries in this hash map extend WeakReference, using
         * its main ref field as the key (which is always a
         * ThreadLocal object).  Note that null keys (i.e. entry.get()
         * == null) mean that the key is no longer referenced, so the
         * entry can be expunged from table.  Such entries are referred to
         * as "stale entries" in the code that follows.
         *
         * 强引用：无论如果都不会被回收，置为null，下一次gc就会被回收
         * 软引用：内存不足就会被回收
         * 弱引用：gc就会被回收
         * 虚引用：
         */
        static class Entry extends WeakReference<ThreadLocal<?>> {
            /** The value associated with this ThreadLocal. */
            Object value;

            /**
             * key是弱引用
             * value是强引用
             * 为啥这样设计：
             *    当某个ThreadLocal失去强引用并且被GC之后，那么其对应的所有的虚引用都会被失效，这样也就获取不到相应值了
             *    在map的角度：就是可以区分哪些Entry是过期的，哪些不是过期的
             */
            Entry(ThreadLocal<?> k, Object v) {
                super(k);
                value = v;
            }
        }

        /**
         * The initial capacity -- MUST be a power of two.
         * 散列表数组初始长度
         * 初始容量16
         * 容量必须是2的整数倍
         */
        private static final int INITIAL_CAPACITY = 16;

        /**
         * The table, resized as necessary.
         * table.length MUST always be a power of two.
         *
         * 散列表的引用
         */
        private Entry[] table;

        /**
         * The number of entries in the table.
         *
         * table中实际Entry的数量
         */
        private int size = 0;

        /**
         * The next size value at which to resize.
         *
         * 扩容阈值  初始是 table.length  *   2/3
         * 达到阈值调用rehash()
         *
         *
         * rehash()返回会做一次全量检查，将过期的entry全部移除，
         * 如果移除之后Entry的数量仍然达到  threshold - (threshold * 4) 就进行扩容
         */
        private int threshold; // Default to 0

        /**
         * Set the resize threshold to maintain at worst a 2/3 load factor.
         *
         * 设置扩容阈值
         */
        private void setThreshold(int len) {
            threshold = len * 2 / 3;
        }


        /**
         * Increment i modulo len.
         * i 当前的下表
         * len 当前table的长度
         *
         * 返回下一个位置 i + 1，如果 i + 1 越界，就返回0   ==>  类似循环数组
         */
        private static int nextIndex(int i, int len) {
            return ((i + 1 < len) ? i + 1 : 0);
        }

        /**
         * Decrement i modulo len.
         * i 当前的下表
         * len 当前table的长度
         *
         * 返回前一个位置 i - 1，如果 i - 1 越界，就返回最后一个下标    ==>  类似循环数组
         */
        private static int prevIndex(int i, int len) {
            return ((i - 1 >= 0) ? i - 1 : len - 1);
        }

        /**
         * Construct a new map initially containing (firstKey, firstValue).
         * ThreadLocalMaps are constructed lazily, so we only create
         * one when we have at least one entry to put in it.
         *
         * 因为Thread.threadLocals变量是延迟初始化的，只有在第一次访问它时才会进行初始化
         *
         * 构造方法：
         * firstKey    TreadLocal对象
         * firstValue  当前线程与ThreadLocal相关联的value
         */
        ThreadLocalMap(ThreadLocal<?> firstKey, Object firstValue) {
            // 创建Entry数组
            table = new Entry[INITIAL_CAPACITY];
            // 计算key的hash值，并找到对应在散列表中的下表
            /**
             * INITIAL_CAPACITY - 1   容量都是2的次方数，-1 对应的二进制都是1
             * 任何数与  INITIAL_CAPACITY - 1  进行与运算，一定是小于等于 INITIAL_CAPACITY - 1 的
             * INITIAL_CAPACITY = 16       10000
             * INITIAL_CAPACITY - 1 = 15   1111
             * 任何数与15 都是小于等于15的
             */
            int i = firstKey.threadLocalHashCode & (INITIAL_CAPACITY - 1);
            // 创建Entry
            table[i] = new Entry(firstKey, firstValue);
            // 实际Entry数量
            size = 1;
            // 设置扩容阈值    INITIAL_CAPACITY  的 2/3
            setThreshold(INITIAL_CAPACITY);
        }

        /**
         * Construct a new map including all Inheritable ThreadLocals
         * from given parent map. Called only by createInheritedMap.
         *
         * @param parentMap the map associated with parent thread.
         */
        private ThreadLocalMap(ThreadLocalMap parentMap) {
            Entry[] parentTable = parentMap.table;
            int len = parentTable.length;
            setThreshold(len);
            table = new Entry[len];

            for (int j = 0; j < len; j++) {
                Entry e = parentTable[j];
                if (e != null) {
                    @SuppressWarnings("unchecked")
                    ThreadLocal<Object> key = (ThreadLocal<Object>) e.get();
                    if (key != null) {
                        Object value = key.childValue(e.value);
                        Entry c = new Entry(key, value);
                        int h = key.threadLocalHashCode & (len - 1);
                        while (table[h] != null)
                            h = nextIndex(h, len);
                        table[h] = c;
                        size++;
                    }
                }
            }
        }

        /**
         * Get the entry associated with key.  This method
         * itself handles only the fast path: a direct hit of existing
         * key. It otherwise relays to getEntryAfterMiss.  This is
         * designed to maximize performance for direct hits, in part
         * by making this method readily inlinable.
         *
         * @param  key the thread local object
         * @return the entry associated with key, or null if no such
         *
         *
         * 代理了ThreadLocal的get()方法
         *
         * key就是ThreadLocal对象
         */
        private Entry getEntry(ThreadLocal<?> key) {
            // 获取下标
            int i = key.threadLocalHashCode & (table.length - 1);
            Entry e = table[i];
            // 获取到的Entry对象不为null，并且就是要查询的key，就返回该Entry对象
            if (e != null && e.get() == key)
                return e;
            else
            /**
             * 获取到的Entry为null
             * 获取到的Entry的key和要查询的key不是同一个key
             *
             *
             * getEntryAfterMiss 会继续向当前桶位后面搜索key为要查询的key 的Entry，
             * 因为在产生Hash冲突的时候，不是用链表的方式来解决，而是线性探测法，继续往后找一个空位放
             *
             * 三个参数：
             * key就是要查询的key
             * i就是key定位到的桶位
             * e就是在桶中获取到的Entry
             */
                return getEntryAfterMiss(key, i, e);
        }

        /**
         * Version of getEntry method for use when key is not found in
         * its direct hash slot.
         *
         * @param  key the thread local object
         * @param  i the table index for key's hash code
         * @param  e the entry at table[i]
         * @return the entry associated with key, or null if no such
         *
         *
         * 三个参数：
         * key就是要查询的key
         * i就是key定位到的桶位
         * e就是在桶中获取到的Entry
         *
         */

        private Entry getEntryAfterMiss(ThreadLocal<?> key, int i, Entry e) {

            Entry[] tab = table;
            int len = tab.length;

            // e 不为null  意味着要查询的key可能以为hash冲突，放到其它位置去了
            while (e != null) {

                // 当前Entry中的key
                ThreadLocal<?> k = e.get();

                // 是要查找的key，那么就可以返回了
                if (k == key)
                    return e;

                // 获取到的Entry中的key是null
                // 因为key是弱引用，可能被回收了，那么get就会得到null
                if (k == null)
                /**
                 * 将所有被gc的key的对应的value也置为null
                 * 简单点说，就是清理过期数据，并且进行一次rehash()
                 *
                 * 因为过期的数据会被清理掉，那么对应的slot就会空出来，如果之前因为hash冲突，让一些key存到了其他地方，
                 * 那么现在清理之后就尝试将那些因为hash冲突被放到其他位置的Entry放到最开始定位到的位置
                 *
                 *
                 * 这也可以解释为什么这里的while循环是e != null
                 * 因为，一个slot如果是null，那么这个slot肯定没有Entry，也没有和这个slot产生hash冲突的Entry，
                 * 因为expungeStaleEntry()会将hash冲突的Entry放到对应的位置（如果对应的位置为null）
                 */
                    expungeStaleEntry(i);
                else
                    // 获取下一个坐标
                    i = nextIndex(i, len);
                // 更新e的引用
                e = tab[i];
            }
            // slot中的Entry就是null，就直接返回null
            // 相连的这个区块都没找到那么就返回null
            return null;
        }

        /**
         * Set the value associated with key.
         *
         * TreadLocal给当前线程添加 TreadLocal-value 的键值对的时候就是调用map的set方法
         *
         * @param key the thread local object
         * @param value the value to be set
         */
        private void set(ThreadLocal<?> key, Object value) {

            // We don't use a fast path as with get() because it is at
            // least as common to use set() to create new entries as
            // it is to replace existing ones, in which case, a fast
            // path would fail more often than not.

            Entry[] tab = table;
            int len = tab.length;
            int i = key.threadLocalHashCode & (len-1);

            /**
             * 找到能替换的key
             */
            for (Entry e = tab[i];
                 e != null;
                 e = tab[i = nextIndex(i, len)]) {
                ThreadLocal<?> k = e.get();

                // 当前Entry的key就是要修改的key，那么就进行修改，并返回
                if (k == key) {
                    e.value = value;
                    return;
                }

                // 当前Entry的key是null，也就是过期数据
                if (k == null) {
                    replaceStaleEntry(key, value, i);
                    return;
                }
            }


            /**
             * 当前map中没有这个key，是新的数据
             * 那么就将其加入到map中
             */
            tab[i] = new Entry(key, value);
            int sz = ++size;
            /**
             * cleanSomeSlots 如果有过期数据被清理，就会返回true，没有返回false
             * sz >= threshold 实际Entry数量大于扩容阈值
             * 进行rehash()
             */
            if (!cleanSomeSlots(i, sz) && sz >= threshold)
                rehash();
        }

        /**
         * Remove the entry for key.
         */
        private void remove(ThreadLocal<?> key) {
            Entry[] tab = table;
            int len = tab.length;
            int i = key.threadLocalHashCode & (len-1);

            for (Entry e = tab[i];
                 e != null;
                 e = tab[i = nextIndex(i, len)]) {

                if (e.get() == key) {
                    // Entry是要remove的这个key，将其删除，并进行一次过期数据清理工作
                    e.clear();
                    expungeStaleEntry(i);
                    return;
                }
            }
        }

        /**
         * Replace a stale entry encountered during a set operation
         * with an entry for the specified key.  The value passed in
         * the value parameter is stored in the entry, whether or not
         * an entry already exists for the specified key.
         *
         * As a side effect, this method expunges all stale entries in the
         * "run" containing the stale entry.  (A run is a sequence of entries
         * between two null slots.)
         *
         * @param  key the key    就是要修改的key
         * @param  value the value to be associated with key    要修改的key的新值
         * @param  staleSlot index of the first stale entry encountered while
         *         searching for key.   第一个过期的slot
         *
         *
         *
         */
        private void replaceStaleEntry(ThreadLocal<?> key, Object value,
                                       int staleSlot) {
            Entry[] tab = table;
            int len = tab.length;
            Entry e;

            // Back up to check for prior stale entry in current run.
            // We clean out whole runs at a time to avoid continual
            // incremental rehashing due to garbage collector freeing
            // up refs in bunches (i.e., whenever the collector runs).
            int slotToExpunge = staleSlot;

            /**
             * 往前找到第一个过期数据的下标，没有就是当前这个slot
             *
             * 直到遇到Entry为null 才跳出循环，
             * 也就是说，如果在出循环之前，有很多个过期的slot，那么slotToExpunge的值会为最后找到的那一个，
             */
            for (int i = prevIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = prevIndex(i, len))

                if (e.get() == null)
                    // 更新过期数据的下标
                    slotToExpunge = i;


            // Find either the key or trailing null slot of run, whichever
            // occurs first
            /**
             * 从当前这个slot往后查找
             */
            for (int i = nextIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = nextIndex(i, len)) {

                ThreadLocal<?> k = e.get();

                // If we find key, then we need to swap it
                // with the stale entry to maintain hash table order.
                // The newly stale slot, or any other stale slot
                // encountered above it, can then be sent to expungeStaleEntry
                // to remove or rehash all of the other entries in run.
                /**
                 * 是我们要修改的key
                 */
                if (k == key) {
                    e.value = value;

                    // 将过期数据放到当前这个位置，再将修改好的Entry放到过期的那个slot
                    // 优化一个位置
                    tab[i] = tab[staleSlot];
                    tab[staleSlot] = e;

                    // Start expunge at preceding stale entry if it exists
                    /**
                     * 满足这个条件，表示前驱，后继都没有找到过期数据
                     * 更新slotToExpunge的值
                     */
                    if (slotToExpunge == staleSlot)
                        slotToExpunge = i;

                    /**
                     * expungeStaleEntry(slotToExpunge) 返回探测遇到的第一个null的位置
                     */
                    cleanSomeSlots(expungeStaleEntry(slotToExpunge), len);
                    return;
                }

                // If we didn't find stale entry on backward scan, the
                // first stale entry seen while scanning for key is the
                // first still present in the run.
                /**
                 * k == null 当前这个Entry是过期数据
                 * slotToExpunge == staleSlot  上面往前查找的过程没有找到过期数据
                 */
                if (k == null && slotToExpunge == staleSlot)
                    // 前提条件是前驱扫描没找到过期数据
                    slotToExpunge = i;
            }


            /**
             * 没有找到一个可以修改的key
             * 那么当前set操作是一个添加新值的操作
             */
            tab[staleSlot].value = null;
            tab[staleSlot] = new Entry(key, value);

            // If there are any other stale entries in run, expunge them
            if (slotToExpunge != staleSlot)
                // 条件成立，说明有过期数据
                cleanSomeSlots(expungeStaleEntry(slotToExpunge), len);
        }

        /**
         * Expunge a stale entry by rehashing any possibly colliding entries
         * lying between staleSlot and the next null slot.  This also expunges
         * any other stale entries encountered before the trailing null.  See
         * Knuth, Section 6.4
         *
         * @param staleSlot index of slot known to have null key
         * @return the index of the next null slot after staleSlot
         * (all between staleSlot and this slot will have been checked
         * for expunging).
         *
         * 因为过期的数据会被清理掉，那么对应的slot就会空出来，如果之前因为hash冲突，让一些key存到了其他地方，
         * 那么现在清理之后就尝试将那些因为hash冲突被放到其他位置的Entry放到最开始定位到的位置，
         * 或者放到离正确位置尽可能近的一个空位
         *
         * expungeStaleEntry会从当前位置往后遍历，遇到过期数据就置为null，不是过期数据就计算其正确下标
         * 如果正确下标与其所在的位置不是同一个位置，那么就是hash冲突了，
         * 那么现在就将其放回正确的位置获取离正确位置尽可能近的一个空位，来优化查询性能
         *
         */
        private int expungeStaleEntry(int staleSlot) {
            Entry[] tab = table;
            int len = tab.length;

            // expunge entry at staleSlot
            tab[staleSlot].value = null;
            tab[staleSlot] = null;
            size--;

            // Rehash until we encounter null
            /**
             * 遇到null就结束循环
             */
            Entry e;
            int i;
            for (i = nextIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = nextIndex(i, len)) {
                ThreadLocal<?> k = e.get();

                if (k == null) {
                    // 过期的Entry
                    e.value = null;
                    tab[i] = null;
                    size--;
                } else {
                    // 没过期的Entry
                    /**
                     * 将与该slot hash冲突的其它Entry放回来这个slot中
                     */
                    int h = k.threadLocalHashCode & (len - 1);
                    if (h != i) {
                        // h != i   表示有hash冲突
                        tab[i] = null;

                        // Unlike Knuth 6.4 Algorithm R, we must scan until
                        // null because multiple entries could have been stale.
                        // 如果还是冲突，那么继续往后找一个位置放，反正要离正确位置尽可能近
                        while (tab[h] != null)
                            h = nextIndex(h, len);
                        tab[h] = e;
                    }
                }
            }
            return i;
        }

        /**
         * Heuristically scan some cells looking for stale entries.
         * This is invoked when either a new element is added, or
         * another stale one has been expunged. It performs a
         * logarithmic number of scans, as a balance between no
         * scanning (fast but retains garbage) and a number of scans
         * proportional to number of elements, that would find all
         * garbage but would cause some insertions to take O(n) time.
         *
         * @param i a position known NOT to hold a stale entry. The
         * scan starts at the element after i.
         *
         * @param n scan control: {@code log2(n)} cells are scanned,
         * unless a stale entry is found, in which case
         * {@code log2(table.length)-1} additional cells are scanned.
         * When called from insertions, this parameter is the number
         * of elements, but when from replaceStaleEntry, it is the
         * table length. (Note: all this could be changed to be either
         * more or less aggressive by weighting n instead of just
         * using straight log n. But this version is simple, fast, and
         * seems to work well.)
         *
         * @return true if any stale entries have been removed.
         *
         * i 开始位置就是expungeStaleEntry方法结束的那个null位置
         * n 两种：table的长度  or  table中Entry的实际数量
         *
         * 清除过期数据
         */
        private boolean cleanSomeSlots(int i, int n) {
            boolean removed = false;
            Entry[] tab = table;
            int len = tab.length;

            do {
                // 直接获取下一个位置，因为最开始这个i是expungeStaleEntry的返回值对应的位置是null
                i = nextIndex(i, len);
                Entry e = tab[i];
                if (e != null && e.get() == null) {
                    // 遇到的slot是过期数据，从这个过期数据开始清理
                    n = len;
                    removed = true;
                    i = expungeStaleEntry(i);
                }
                // 结束清理的条件
            } while ( (n >>>= 1) != 0);
            return removed;
        }

        /**
         * Re-pack and/or re-size the table. First scan the entire
         * table removing stale entries. If this doesn't sufficiently
         * shrink the size of the table, double the table size.
         */
        private void rehash() {
            // 进行全表过期数据的清理工作
            expungeStaleEntries();

            // Use lower threshold for doubling to avoid hysteresis
            // 清理结束之后，size还大于阈值的3/4，就进行扩容
            if (size >= threshold - threshold / 4)
                resize();
        }

        /**
         * Double the capacity of the table.
         */
        private void resize() {
            Entry[] oldTab = table;
            int oldLen = oldTab.length;
            int newLen = oldLen * 2;

            // 创建一个更大的表，以前的两倍
            Entry[] newTab = new Entry[newLen];
            int count = 0;

            // 遍历老表，迁移数据到新表
            for (int j = 0; j < oldLen; ++j) {
                Entry e = oldTab[j];
                if (e != null) {
                    ThreadLocal<?> k = e.get();
                    if (k == null) {
                        // 过期数据
                        e.value = null; // Help the GC
                    } else {
                        // 不是过期数据，重新计算下标
                        int h = k.threadLocalHashCode & (newLen - 1);
                        while (newTab[h] != null)
                            // hash冲突了
                            h = nextIndex(h, newLen);
                        newTab[h] = e;
                        // Entry 计数
                        count++;
                    }
                }
            }

            // 更新扩容阈值
            setThreshold(newLen);
            size = count;
            table = newTab;
        }

        /**
         * Expunge all stale entries in the table.
         */
        private void expungeStaleEntries() {
            Entry[] tab = table;
            int len = tab.length;
            for (int j = 0; j < len; j++) {
                Entry e = tab[j];
                if (e != null && e.get() == null)
                    expungeStaleEntry(j);
            }
        }
    }
}
