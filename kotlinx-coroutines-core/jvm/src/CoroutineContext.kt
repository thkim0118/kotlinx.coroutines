/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")

package kotlinx.coroutines

import kotlinx.coroutines.internal.*
import kotlinx.coroutines.scheduling.*
import kotlin.coroutines.*
import kotlin.coroutines.jvm.internal.*

internal const val COROUTINES_SCHEDULER_PROPERTY_NAME = "kotlinx.coroutines.scheduler"

internal val useCoroutinesScheduler = systemProp(COROUTINES_SCHEDULER_PROPERTY_NAME).let { value ->
    when (value) {
        null, "", "on" -> true
        "off" -> false
        else -> error("System property '$COROUTINES_SCHEDULER_PROPERTY_NAME' has unrecognized value '$value'")
    }
}

internal actual fun createDefaultDispatcher(): CoroutineDispatcher =
    if (useCoroutinesScheduler) DefaultScheduler else CommonPool

/**
 * Creates context for the new coroutine. It installs [Dispatchers.Default] when no other dispatcher nor
 * [ContinuationInterceptor] is specified, and adds optional support for debugging facilities (when turned on).
 *
 * See [DEBUG_PROPERTY_NAME] for description of debugging facilities on JVM.
 */
@ExperimentalCoroutinesApi
public actual fun CoroutineScope.newCoroutineContext(context: CoroutineContext): CoroutineContext {
    val combined = coroutineContext + context
    val debug = if (DEBUG) combined + CoroutineId(COROUTINE_ID.incrementAndGet()) else combined
    return if (combined !== Dispatchers.Default && combined[ContinuationInterceptor] == null)
        debug + Dispatchers.Default else debug
}

/**
 * Executes a block using a given coroutine context.
 */
internal actual inline fun <T> withCoroutineContext(context: CoroutineContext, countOrElement: Any?, block: () -> T): T {
    val oldValue = updateThreadContext(context, countOrElement)
    try {
        return block()
    } finally {
        restoreThreadContext(context, oldValue)
    }
}

/**
 * Executes a block using a context of a given continuation.
 */
internal actual inline fun <T> withContinuationContext(continuation: Continuation<*>, countOrElement: Any?, block: () -> T): T {
    val context = continuation.context
    val oldValue = updateThreadContext(context, countOrElement)
    val undispatchedCompletion = if (oldValue !== NO_THREAD_ELEMENTS) {
        // Only if some values were replaced we'll go to the slow path of figuring out where/how to restore them
        continuation.undispatchedCompletion()
    } else
        null // fast path -- don't even try to find undispatchedCompletion as there's nothing to restore in the context
    undispatchedCompletion?.saveThreadContext(context, oldValue)
    try {
        return block()
    } finally {
        if (undispatchedCompletion == null || undispatchedCompletion.clearThreadContext())
            restoreThreadContext(context, oldValue)
    }
}

internal tailrec fun Continuation<*>.undispatchedCompletion(): UndispatchedCoroutine<*>? {
    // Find direct completion of this continuation
    val completion: Continuation<*> = when (this) {
        is BaseContinuationImpl -> completion ?: return null // regular suspending function -- direct resume
        is DispatchedCoroutine -> return null // dispatches on resume
        is ScopeCoroutine -> uCont // other scoped coroutine -- direct resume
        else -> return null // something else -- not supported
    }
    if (completion is UndispatchedCoroutine<*>) return completion // found UndispatchedCoroutine!
    return completion.undispatchedCompletion() // walk up the call stack with tail call
}

// Used by withContext when context changes, but dispatcher stays the same
internal actual class UndispatchedCoroutine<in T> actual constructor(
    context: CoroutineContext,
    uCont: Continuation<T>
) : ScopeCoroutine<T>(context, uCont) {
    private var savedContext: CoroutineContext? = null
    private var savedOldValue: Any? = null

    fun saveThreadContext(context: CoroutineContext, oldValue: Any?) {
        savedContext = context
        savedOldValue = oldValue
    }

    fun clearThreadContext(): Boolean {
        if (savedContext == null) return false
        savedContext = null
        savedOldValue = null
        return true
    }

    override fun afterResume(state: Any?) {
        savedContext?.let { context ->
            restoreThreadContext(context, savedOldValue)
            savedContext = null
            savedOldValue = null
        }
        // resume undispatched -- update context but stay on the same dispatcher
        val result = recoverResult(state, uCont)
        withContinuationContext(uCont, null) {
            uCont.resumeWith(result)
        }
    }
}

internal actual val CoroutineContext.coroutineName: String? get() {
    if (!DEBUG) return null
    val coroutineId = this[CoroutineId] ?: return null
    val coroutineName = this[CoroutineName]?.name ?: "coroutine"
    return "$coroutineName#${coroutineId.id}"
}

private const val DEBUG_THREAD_NAME_SEPARATOR = " @"

internal data class CoroutineId(
    val id: Long
) : ThreadContextElement<String>, AbstractCoroutineContextElement(CoroutineId) {
    companion object Key : CoroutineContext.Key<CoroutineId>
    override fun toString(): String = "CoroutineId($id)"

    override fun updateThreadContext(context: CoroutineContext): String {
        val coroutineName = context[CoroutineName]?.name ?: "coroutine"
        val currentThread = Thread.currentThread()
        val oldName = currentThread.name
        var lastIndex = oldName.lastIndexOf(DEBUG_THREAD_NAME_SEPARATOR)
        if (lastIndex < 0) lastIndex = oldName.length
        currentThread.name = buildString(lastIndex + coroutineName.length + 10) {
            append(oldName.substring(0, lastIndex))
            append(DEBUG_THREAD_NAME_SEPARATOR)
            append(coroutineName)
            append('#')
            append(id)
        }
        return oldName
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: String) {
        Thread.currentThread().name = oldState
    }
}
