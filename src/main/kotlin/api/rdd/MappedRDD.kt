package api.rdd

import worker.WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class MappedRDD<T, R>(val parent: RDD<T>, val f: suspend (T) -> R): RDD<R>(parent.master) {
    override fun toImpl(): RDDImpl<R> {
        return MappedRDDImpl(parent.toImpl(), f)
    }
}

class MappedRDDImpl<T, R>(val parent: RDDImpl<T>, val f: suspend (T) -> R): RDDImpl<R>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<R> {
        val channel = Channel<R>(MAX_CAP)
        val recChannel = parent.channel(scope, ctx)
        scope.launch {
            (1..CONCURRENT_MAP_LIMIT).map {
                async {
                    for (t in recChannel) {
                        channel.send(f(t))
                    }
                }
            }.awaitAll()
            channel.close()
        }
        return channel
    }

    companion object {
        const val CONCURRENT_MAP_LIMIT = 10
    }
}
