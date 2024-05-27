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

/**
 * Provides a non-blocking lock mechanism optimized for scenarios with one writer and multiple readers.
 * This class implements a version of a stamped lock that supports optimistic reads and non-blocking writes.
 * <p>
 * Key Features:
 * <ul>
 *   <li><b>Optimistic Reading:</b> Readers can obtain a stamp that represents the lock state at a point in time.
 *  After performing the read operations, readers should validate the stamp to check if the read data is consistent.
 *  If the stamp is invalid, indicating a write occurred, the read operation should be retried.</li>
 *  <li><b>Non-blocking Writer:</b> Writer always acquires the write lock without blocking. This lock does not provide
 *  * exclusive access to the write lock. Users must ensure that the write lock is not already held before calling the
 *  * write lock method.</li>
 *  <li><b>Blocking readers:</b> Readers are blocked while a writer lock is acquired.</li>
 * </ul>
 * <p>
 * Usage: Readers should always validate their stamps after reading data to ensure no write operations have invalidated their
 * read.
 * <p>
 * Note:
 * <ul>
 *     <li>The lock is not reentrant. Care must be taken to avoid recursive lock acquisition.</li>
 *     <li>The implementation uses busy-waiting in the optimistic read lock acquisition.</li>
 * </ul>
 */
@SuppressWarnings("NonAtomicOperationOnVolatileField")
public final class SingleWriterStampedSpinLock {
    private volatile long stamp = 0;

    /**
     * Acquires a read lock if available. If the lock is not available, the method will busy-wait until the lock is available.
     * <p>
     * The method returns a stamp representing the lock state at the time of acquiring the lock. The stamp should be used to
     * validate the read data after reading.
     *
     * @return the stamp representing the lock state at the time of acquiring the lock
     * @see #validateReadLock(long)
     */
    public long optimisticReadLock() {
        for (; ; ) {
            long currentStamp = this.stamp;
            if (writeLockAvailable(currentStamp)) {
                // todo: consider backoff
                return currentStamp;
            }
        }
    }

    /**
     * Validates the read lock using the stamp obtained from {@link #optimisticReadLock()}.
     * <p>
     * The method returns true if the stamp is valid, indicating the read data is consistent. If the stamp is invalid,
     * indicating a write operation occurred, the method returns false.
     *
     * @param stamp the stamp obtained from {@link #optimisticReadLock()}
     * @return true if the stamp is valid, false otherwise
     */
    public boolean validateReadLock(long stamp) {
        return this.stamp == stamp;
    }

    /**
     * Acquires a write lock.
     * <p>
     * Users must ensure that the write lock is not already held before calling this method. It does not provide
     * exclusive access to the write lock. User must call {@link #writeUnlock()} to release the write lock.
     */
    public void writeLock() {
        assert writeLockAvailable(stamp) : "write lock already held";
        stamp++;
    }

    /**
     * Releases the write lock. Users must ensure that the write lock is held before calling this method.
     */
    public void writeUnlock() {
        assert !writeLockAvailable(stamp) : "write lock not held";
        stamp++;
    }

    private static boolean writeLockAvailable(long stamp) {
        return (stamp & 1) == 0;
    }
}
