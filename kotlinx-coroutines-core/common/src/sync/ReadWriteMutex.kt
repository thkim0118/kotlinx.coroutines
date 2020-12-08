/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlin.js.*

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
@JsName("_ReadWriteMutex")
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
@OptIn(HazardousConcurrentApi::class)
internal class ReadWriteMutexImpl : ReadWriteMutex {
    private val WR = atomic(0)
    private val STATE = atomic(0L)

    private val sqsWriters = WritersSQS()

    private inner class WritersSQS : SegmentQueueSynchronizer<Unit>() {
        override val resumeMode get() = ResumeMode.ASYNC
        override val cancellationMode get() = CancellationMode.SMART_ASYNC

        override fun onCancellation(): Boolean {
            while (true) {
                val state = STATE.value
                if (state.ww == 0) return false
                if (state.ww == 1 && !state.wla && !state.wlrp) {
                    if (STATE.compareAndSet(state, constructState(state.ar + 1, false, 0, true))) {
                        completeWriteUnlock()
                        return true
                    }
                } else {
                    if (STATE.compareAndSet(state, constructState(state.ar, state.wla, state.ww - 1, state.wlrp)))
                        return true
                }
            }
        }

        // Fails since we have not changed the state in `onCancellation()`,
        // the value should be returned by `returnValue()` then.
        override fun tryReturnRefusedValue(value: Unit) = false

        override fun returnValue(value: Unit) = writeUnlock()
    }

    private val sqsReaders = ReadersSQS()

    private inner class ReadersSQS : SegmentQueueSynchronizer<Unit>() {
        override val resumeMode get() = ResumeMode.ASYNC
        override val cancellationMode get() = CancellationMode.SMART_ASYNC

        override fun onCancellation(): Boolean {
            while (true) {
                val wr = WR.value
                if (wr == 0) return false
                if (WR.compareAndSet(wr, wr - 1)) return true
            }
        }

        // Fails since we have not changed the state in `onCancellation()`,
        // the value should be returned by `returnValue()` then.
        override fun tryReturnRefusedValue(value: Unit) = false

        override fun returnValue(value: Unit) = readUnlock()
    }

    @HazardousConcurrentApi
    override suspend fun readLock() {
        if (tryAcquireReadLock()) return
        WR.incrementAndGet()
        while (true) {
            val state = STATE.value
            // Is there a writer holding the lock or waiting for it?
            if (state.wla || state.ww > 0) {
                suspendCancellableCoroutine<Unit> { sqsReaders.suspend(it) }
                return
            } else {
                while (true) {
                    val wr = WR.value
                    if (wr == 0) {
                        suspendCancellableCoroutine<Unit> { sqsReaders.suspend(it) }
                        return
                    } else if (WR.compareAndSet(wr, wr - 1)) {
                        readLock() // try again
                        return
                    }
                }
            }
        }
    }

    private fun tryAcquireReadLock(): Boolean {
        while (true) {
            val state = STATE.value
            if (state.wla || state.ww > 0) return false
            if (STATE.compareAndSet(state, constructState(state.ar + 1, false, 0, state.wlrp)))
                return true
        }
    }

    @HazardousConcurrentApi
    override fun readUnlock() {
        while (true) {
            val state = STATE.value
            if (state.ar == 1) {
                val resumeWriter = state.ww > 0
                if (state.wlrp) {
                    if (STATE.compareAndSet(state, constructState(0, false, state.ww, true)))
                        return
                } else if (resumeWriter) {
                    if (STATE.compareAndSet(state, constructState(0, true, state.ww - 1, false))) {
                        sqsWriters.resume(Unit)
                        return
                    }
                } else {
                    if (STATE.compareAndSet(state, constructState(0, false, 0, false))) {
                        return
                    }
                }
            } else {
                assert { !state.wla }
                if (STATE.compareAndSet(state, constructState(state.ar - 1, false, state.ww, state.wlrp))) {
                    return
                }
            }
        }
    }

    @HazardousConcurrentApi
    override suspend fun writeLock() {
        while (true) {
            val state = STATE.value
            if (!state.wla && !state.wlrp && state.ar == 0) {
                assert { state.ww == 0 }
                if (STATE.compareAndSet(state, constructState(0, true, 0, false))) {
                    return
                }
            } else {
                if (STATE.compareAndSet(state, constructState(state.ar, state.wla, state.ww + 1, state.wlrp))) {
                    suspendCancellableCoroutine<Unit> { cont -> sqsWriters.suspend(cont) }
                    return
                }
            }
        }
    }

    @HazardousConcurrentApi
    override fun writeUnlock() {
        while (true) {
            val state = STATE.value
            if (state.ww > 0) {
                if (STATE.compareAndSet(state, constructState(0, true, state.ww - 1, false))) {
                    sqsWriters.resume(Unit)
                    return
                }
            } else {
                if (STATE.compareAndSet(state, constructState(1, false, 0, true))) {
                    completeWriteUnlock()
                    return
                }
            }
        }
    }

    private fun completeWriteUnlock() {
        val wr = WR.getAndSet(0)
        STATE.update { state2 -> constructState(state2.ar + wr, false, state2.ww, true) }
        repeat(wr) { sqsReaders.resume(Unit) }
        STATE.update { state2 -> constructState(state2.ar, false, state2.ww, false) }
        readUnlock()
        if (WR.value > 0) {
            while (true) {
                val state3 = STATE.value
                if (state3.wla || state3.ww > 0 || state3.wlrp) return
                if (STATE.compareAndSet(state3, constructState(state3.ar + 1, false, 0, true))) {
                    completeWriteUnlock()
                    return
                }
            }
        }
    }

    internal val stateRepresentation: String get() =
        "<wr=${WR.value},ar=${STATE.value.ar}" +
        ",wla=${STATE.value.wla},ww=${STATE.value.ww}" +
        ",wlrp=${STATE.value.wlrp}" +
        ",sqs_r=$sqsReaders,sqs_w=$sqsWriters>"
}

private fun constructState(activeReaders: Int, wla: Boolean, waitingWriters: Int, wlrp: Boolean): Long =
    (if (wla) WLA_BIT else 0) +
    (if (wlrp) WLRP_BIT else 0) +
    activeReaders * AR_MULTIPLIER +
    waitingWriters * WW_MULTIPLIER

private val Long.wla: Boolean get() = this or WLA_BIT == this
private val Long.wlrp: Boolean get() = this or WLRP_BIT == this
private val Long.ww: Int get() = ((this % AR_MULTIPLIER) / WW_MULTIPLIER).toInt()
private val Long.ar: Int get() = (this / AR_MULTIPLIER).toInt()

private const val WLA_BIT = 1L
private const val WLRP_BIT = 2L
private const val WW_MULTIPLIER = 1024L
private const val AR_MULTIPLIER = 1024L * 1024L