/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer

/**
 * A readers-writer mutex maintains a logical pair of locks, one for
 * read-only operations, that can be processed concurrently, and one
 * for write operations which guarantees an exclusive access so that
 * neither write or read operation can be processed in parallel.
 *
 * Similarly to [Mutex], this readers-writer mutex is  **non-reentrant**,
 * that is invoking [readLock] or [writeLock] even from the same thread or
 * coroutine that currently holds the corresponding lock still suspends the invoker.
 * At the same time, invoking [readLock] from the holder of the write lock
 * also suspends the invoker.
 *
 * The typical usage of [ReadWriteMutex] is wrapping each read invocation with
 * [ReadWriteMutex.withReadLock] and each write invocation with [ReadWriteMutex.withWriteLock]
 * correspondingly. These wrapper functions guarantee that the mutex is used
 * correctly and safely. However, one can use `lock` and `unlock` operations directly,
 * but there is a contract that `unlock` should be invoked only after a successful
 * corresponding `lock` invocation. Since this low-level API is potentially error-prone,
 * it is marked as [HazardousConcurrentApi] and requires the corresponding [OptIn] declaration.
 *
 * The advantage of using [ReadWriteMutex] comparing to plain [Mutex] is an
 * availability to parallelize read operations and, therefore, increasing the
 * level of concurrency. It is extremely useful for the workloads with dominating
 * read operations so that they can be executed in parallel and improve the
 * performance. However, depending on the updates frequence, the execution cost of
 * read and write operations, and the contention, it can be cheaper to use a plain [Mutex].
 * Therefore, it is highly recommended to measure the performance difference
 * to make a right choice.
 */
public interface ReadWriteMutex {
    /**
     * Acquires a read lock of this mutex if the write lock is not acquired,
     * suspends the caller otherwise until the write lock is released. TODO fairness
     *
     * This suspending function is cancellable. If the [Job] of the current coroutine is cancelled or completed while this
     * function is suspended, this function immediately resumes with [CancellationException].
     * There is a **prompt cancellation guarantee**. If the job was cancelled while this function was
     * suspended, it will not resume successfully. See [suspendCancellableCoroutine] documentation for low-level details.
     * This function releases the lock if it was already acquired by this function before the [CancellationException]
     * was thrown.
     *
     * Note that this function does not check for cancellation when it is not suspended.
     * Use [yield] or [CoroutineScope.isActive] to periodically check for cancellation in tight loops if needed.
     *
     * TODO HazardousConcurrentApi
     */
    @HazardousConcurrentApi
    public suspend fun readLock()

    /**
     * Releases a read lock of this mutex and resumes the first waiting writer
     * if there is the one and this operation releases the last read lock.
     *
     * TODO HazardousConcurrentApi
     */
    @HazardousConcurrentApi
    public fun readUnlock()

    /**
     * TODO
     */
    @HazardousConcurrentApi
    public suspend fun writeLock()

    /**
     * TODO
     */
    @HazardousConcurrentApi
    public fun writeUnlock()
}

/**
 * Creates a new [ReadWriteMutex] instance,
 * both read and write locks are not acquired.
 *
 * TODO: fairness
 */
public fun ReadWriteMutex(): ReadWriteMutex = ReadWriteMutexImpl()

/**
 * Executes the given [action] under a _read_ mutex's lock.
 *
 * @return the return value of the [action].
 */
@OptIn(HazardousConcurrentApi::class)
public suspend inline fun <T> ReadWriteMutex.withReadLock(action: () -> T): T {
    readLock()
    try {
       return action()
    } finally {
        readUnlock()
    }
}

/**
 * Executes the given [action] under the _write_ mutex's lock.
 *
 * @return the return value of the [action].
 */
@OptIn(HazardousConcurrentApi::class)
public suspend inline fun <T> ReadWriteMutex.withWriteLock(action: () -> T): T {
    writeLock()
    try {
        return action()
    } finally {
        writeUnlock()
    }
}

/**
 * This readers-writer mutex maintains two atomic variables [R] and [W], and uses two
 * separate [SegmentQueueSynchronizer]-s for waiting readers and writers. The 64-bit
 * variable [R] maintains three mostly readers-related states atomically:
 * - `AWF` (active writer flag) bit that is `true` if there is a writer holding the write lock.
 * - `WWF` (waiting writer flag) bit that is `true` if there is a writer waiting for the write lock
 *                               and the lock is not acquired due to active readers.
 * - `AR` (active readers) 30-bit counter which represents the number of coroutines holding a read lock,
 * - `WR` (waiting readers) 30-bit counter which represents the number of coroutines waiting for a
 *                          read lock in the corresponding [SegmentQueueSynchronizer].
 * This way, when a reader comes for a lock, it atomically checks whether the `WF` flag is set and
 * either increments the `AR` counter and acquires a lock if it is not set, or increments the
 * `WR` counter and suspends otherwise. At the same time, when a reader releases the lock, it
 * it checks whether it is the last active reader and resumes the first  waiting writer if the `WF`
 * flag is set.
 *
 * Writers, on their side, use an additional [W] field which represents the number of waiting
 * writers as well as several internal flags:
 * - `WW` (waiting writers) 30-bit counter that represents the number of coroutines  waiting for
 *                          the write lock in the corresponding [SegmentQueueSynchronizer],
 * - `WLA` (the write lock is acquired) flag which is `true` when the write lock is acquired,
 *                                      and `WF` should be `true` as well in this case,
 * - `WLRP` (write lock release is in progress) flag which is `true` when the [releaseWrite]
 *                                              invocation is in progress. Note, that `WLA`
 *                                              should already be `false` in this case.
 * - `WRF` (writer is resumed) flag that can be set to `true` by [releaseRead] if there is a
 *                             concurrent [releaseWrite], so that `WLRP` is set to true. This
 *                             flag helps to manage the race when [releaseWrite] successfully
 *                             resumed waiting readers, has not re-set `WF` flag in [R] yet,
 *                             while there readers completed with [releaseRead] and the last
 *                             one observes the `WF` flag set to `true`, so that it should try
 *                             to resume the next waiting writer. However, it is better to tell
 *                             the concurrent [releaseWrite] to check whether there is a writer
 *                             and resume it.
 *
 */
internal class ReadWriteMutexImpl : ReadWriteMutex {
    private val AR = atomic(0)
    private val STATE = atomic(0L)

    @HazardousConcurrentApi
    override suspend fun readLock() {
        // 1. Increment the number of readers at first
        AR.getAndIncrement()
        // 2. Try to set `RLA` flag, increment `WR` otherwise
        val acquired: Boolean
        while (true) {
            val state = STATE.value
            // Is the write lock acquired or is there a waiting writer?
            if (state.wla || state.ww > 0) {
                // This `readLock` invocation should suspend,
                // change the state correspondingly.
                if (STATE.compareAndSet(state, constructState(state.rla, state.wr + 1, state.wla, state.ww))) {
                    acquired = false
                    break
                }
            } else {
                // There is no writer waiting or holding the lock,
                // try to grab å reader lock by setting `RLA` flag
                // if it is not already set.
                if (state.rla) {
                    acquired = true
                    break
                }
                if (STATE.compareAndSet(state, constructState(true, state.wr, state.wla, state.ww))) {
                    acquired = true
                    break
                }
            }
        }
        // 3. Complete the operation immediately if the lock is successfully acquired.
        if (acquired) return
        // 4. This operation should suspend, decrement the initially incremented `AR`
        while (true) {
            val ar = AR.value
            if (ar == 1) {
                // check RLA again!
                while (true) {
                    val state = STATE.value
                    if (state.wr == 0) {
                        AR.decrementAndGet()
                        suspendCancellableCoroutine<Unit> { sqsReaders.suspend(it) }
                        return
                    }
                    if (state.rla) {
                        if (STATE.compareAndSet(state, constructState(true, state.wr - 1, false, state.ww)))
                            return
                    } else {
                        AR.decrementAndGet()
                        suspendCancellableCoroutine<Unit> { sqsReaders.suspend(it) }
                        return
                    }
                }
            } else {
                if (AR.compareAndSet(ar, ar - 1)) {
                    suspendCancellableCoroutine<Unit> { sqsReaders.suspend(it) }
                    return
                }
            }
        }
    }

    @HazardousConcurrentApi
    override fun readUnlock() {
        while (true) {
            val ar = AR.value
            if (ar > 1) {
                if (AR.compareAndSet(ar, ar - 1)) return
            } else {
                while (true) {
                    val state = STATE.value
                    if (state.ww == 0) {
                        if (STATE.compareAndSet(state, constructState(false, state.wr, false, 0))) {
                            AR.decrementAndGet()
                            return
                        }
                    } else {
                        if (STATE.compareAndSet(state, constructState(false, state.wr, true, state.ww - 1))) {
                            AR.decrementAndGet()
                            sqsWriters.resume(Unit)
                            return
                        }
                    }
                }
            }
        }
    }

    private val sqsWriters = object: SegmentQueueSynchronizer<Unit>() {
        override val resumeMode get() = ResumeMode.ASYNC
        override val cancellationMode get() = CancellationMode.SIMPLE
    }

    private val sqsReaders = object: SegmentQueueSynchronizer<Unit>() {
        override val resumeMode get() = ResumeMode.ASYNC
        override val cancellationMode get() = CancellationMode.SIMPLE
    }

    @HazardousConcurrentApi
    override suspend fun writeLock() {
        while (true) {
            val state = STATE.value
            if (state.wla || state.rla) {
                if (STATE.compareAndSet(state, constructState(state.rla, state.wr, state.wla, state.ww + 1)))
                    break
            } else {
                if (STATE.compareAndSet(state, constructState(false, 0, true, 0)))
                    return
            }
        }
        suspendCancellableCoroutine<Unit> { sqsWriters.suspend(it) }
    }

    @HazardousConcurrentApi
    override fun writeUnlock() {
        while (true) {
            val state = STATE.value
            // Let's release readers at first (just for the demo)
            if (state.ww > 0) {
                if (STATE.compareAndSet(state, constructState(false, state.wr, true, state.ww - 1))) {
                    sqsWriters.resume(Unit)
                    return
                }
            } else {
                val wr = state.wr
                val rla = wr > 0
                if (STATE.compareAndSet(state, constructState(rla, 0, false, state.ww))) {
                    AR.getAndAdd(state.wr)
                    repeat(state.wr) { sqsReaders.resume(Unit) }
                    return
                }
            }
        }
    }

    internal val stateRepresentation: String get() =
        "ar=${AR.value}" +
        ",rla=${STATE.value.rla},wla=${STATE.value.wla}" +
        ",wr=${STATE.value.wr},ww=${STATE.value.ww}" +
        ",sqs_r=$sqsReaders,sqs_w=$sqsWriters"
}

private fun constructState(rla: Boolean, waitingReaders: Int, wla: Boolean, waitingWriters: Int): Long =
    (if (rla) RLA_BIT else 0) +
    (if (wla) WLA_BIT else 0) +
    waitingReaders * WR_MULTIPLIER +
    waitingWriters * WW_MULTIPLIER

private val Long.rla: Boolean get() = this or RLA_BIT == this
private val Long.wla: Boolean get() = this or WLA_BIT == this
private val Long.ww: Int get() = ((this % WR_MULTIPLIER) / WW_MULTIPLIER).toInt()
private val Long.wr: Int get() = (this / WR_MULTIPLIER).toInt()

private const val WLA_BIT = 1L
private const val RLA_BIT = 2L
private const val WW_MULTIPLIER = 100L
private const val WR_MULTIPLIER = 100_000L