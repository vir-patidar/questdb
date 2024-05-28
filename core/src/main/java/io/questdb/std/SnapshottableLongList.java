/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

import io.questdb.cairo.BinarySearch;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;

public class SnapshottableLongList implements Mutable, LongVec, Sinkable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final long DEFAULT_NO_ENTRY_VALUE = -1L;
    private final SingleWriterStampedSpinLock lock = new SingleWriterStampedSpinLock();
    private final long noEntryValue;
    private long[] data;
    private long lastSnapshotStamp = -1;
    private int pos = 0;
    private SnapshottableLongList snapshot;

    public SnapshottableLongList(int capacity) {
        this(capacity, DEFAULT_NO_ENTRY_VALUE);
    }

    public SnapshottableLongList(int capacity, long noEntryValue) {
        this.data = new long[capacity];
        this.noEntryValue = noEntryValue;
    }

    public SnapshottableLongList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        lock.writeLock();
        try {
            System.arraycopy(data, srcPos, data, dstPos, length);
        } finally {
            lock.writeUnlock();
        }
    }

    public int binarySearchBlock(int shl, long value, int scanDir) {
        // Binary searches using 2^shl blocks
        // e.g. when shl == 2
        // this method treats 4 longs as 1 entry
        // taking first long for the comparisons
        // and ignoring the other 3 values.

        // This is useful when list is a dictionary where first long is a key
        // and subsequent X (1, 3, 7 etc.) values are the value of the dictionary.

        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync

        return binarySearchBlock(0, shl, value, scanDir);
    }

    public int binarySearchBlock(int offset, int shl, long value, int scanDir) {
        int low = offset >> shl;
        int high = (pos - 1) >> shl;
        while (high - low > 65) {
            final int mid = (low + high) >>> 1;
            final long midVal = data[mid << shl];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == BinarySearch.SCAN_UP ?
                        scrollUpBlock(shl, mid, midVal) :
                        scrollDownBlock(shl, mid, high, midVal);
            }
        }
        return scanDir == BinarySearch.SCAN_UP ?
                scanUpBlock(shl, value, low, high + 1) :
                scanDownBlock(shl, value, low, high + 1);
    }

    public int capacity() {
        return data.length;
    }

    public void clear() {
        lock.writeLock();
        pos = 0;
        lock.writeUnlock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof SnapshottableLongList && equals((SnapshottableLongList) that);
    }

    public long get(int index) {
        if (index < pos) {
            return data[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    /**
     * Returns element at the specified position. This method does not do
     * bounds check and may cause memory corruption if index is out of bounds.
     * Instead the responsibility to check bounds is placed on application code,
     * which is often the case anyway, for example in indexed for() loop.
     *
     * @param index of the element
     * @return element at the specified position.
     */
    public long getQuick(int index) {
        return data[index];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        long hashCode = 1;
        for (int i = 0, n = pos; i < n; i++) {
            long v = getQuick(i);
            hashCode = 31 * hashCode + (v == noEntryValue ? 0 : v);
        }
        return (int) hashCode;
    }

    public int indexOf(long o) {
        for (int i = 0, n = pos; i < n; i++) {
            if (o == getQuick(i)) {
                return i;
            }
        }
        return -1;
    }

    public void insert(int index, int length) {
        lock.writeLock();
        try {
            checkCapacity(pos + length);
            if (pos > index) {
                System.arraycopy(data, index, data, index + length, pos - index);
            }
            pos += length;
        } finally {
            lock.writeUnlock();
        }
    }

    @Override
    public LongVec newInstance() {
        SnapshottableLongList newList = new SnapshottableLongList(size());
        newList.setPos(pos);
        return newList;
    }

    public void set(int index, long element) {
        lock.writeLock();
        try {
            if (index < pos) {
                data[index] = element;
                return;
            }
            throw new ArrayIndexOutOfBoundsException(index);
        } finally {
            lock.writeUnlock();
        }
    }

    public final void setPos(int pos) {
        lock.writeLock();
        try {
            checkCapacity(pos);
            this.pos = pos;
        } finally {
            lock.writeUnlock();
        }
    }

    public void setQuick(int index, long value) {
        assert index < pos;
        lock.writeLock();
        data[index] = value;
        lock.writeUnlock();
    }

    public int size() {
        return pos;
    }

    public SnapshottableLongList snapshot() {
        if (snapshot == null) {
            snapshot = new SnapshottableLongList(data.length, noEntryValue);
        }
        long stamp = lock.optimisticReadLock();
        if (stamp == lastSnapshotStamp) {
            return snapshot;
        }
        for (; ; ) {
            long[] currentData = data;

            if (snapshot.data.length != currentData.length) {
                snapshot.data = new long[currentData.length];
            }
            System.arraycopy(currentData, 0, snapshot.data, 0, currentData.length);
            snapshot.pos = pos;

            if (lock.validateReadLock(stamp)) {
                lastSnapshotStamp = stamp;
                return snapshot;
            }
            // spin until we have consistent snapshot
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        for (int i = 0, k = pos; i < k; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(get(i));
        }
        sink.putAscii(']');
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final Utf16Sink sb = Misc.getThreadLocalSink();
        toSink(sb);
        return sb.toString();
    }

    private void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        int l = data.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            long[] buf = new long[newCap];
            System.arraycopy(data, 0, buf, 0, l);
            this.data = buf;
        }
    }

    private boolean equals(SnapshottableLongList that) {
        if (this.pos != that.pos) {
            return false;
        }
        if (this.noEntryValue != that.noEntryValue) {
            return false;
        }
        for (int i = 0, n = pos; i < n; i++) {
            if (this.getQuick(i) != that.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    private int scanDownBlock(int shl, long v, int low, int high) {
        for (int i = high - 1; i >= low; i--) {
            long that = data[i << shl];
            if (that == v) {
                return i << shl;
            }
            if (that < v) {
                return -(((i + 1) << shl) + 1);
            }
        }
        return -((low << shl) + 1);
    }

    private int scanUpBlock(int shl, long value, int low, int high) {
        for (int i = low; i < high; i++) {
            long that = data[i << shl];
            if (that == value) {
                return i << shl;
            }
            if (that > value) {
                return -((i << shl) + 1);
            }
        }
        return -((high << shl) + 1);
    }

    private int scrollDownBlock(int shl, int low, int high, long value) {
        do {
            if (low < high) {
                low++;
            } else {
                return low << shl;
            }
        } while (data[low << shl] == value);
        return (low - 1) << shl;
    }

    private int scrollUpBlock(int shl, int high, long value) {
        do {
            if (high > 0) {
                high--;
            } else {
                return 0;
            }
        } while (data[high << shl] == value);
        return (high + 1) << shl;
    }
}
