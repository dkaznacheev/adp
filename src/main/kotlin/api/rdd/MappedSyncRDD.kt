package api.rdd

import worker.WorkerContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.map

class MappedSyncRDD<T, R>(val parent: RDD<T>, val f: suspend (T) -> R): RDD<R>(parent.master) {
    override fun toImpl(): RDDImpl<R> {
        return MappedSyncRDDImpl(parent.toImpl(), f)
    }
}

class MappedSyncRDDImpl<T, R>(val parent: RDDImpl<T>, val f: suspend (T) -> R): RDDImpl<R>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<R> {
        return parent.channel(scope, ctx).map { f(it) }
    }
}
