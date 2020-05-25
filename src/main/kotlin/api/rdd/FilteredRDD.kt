package api.rdd

import api.CONCURRENT_MAP_LIMIT
import worker.WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class FilteredRDD<T>(val parent: RDD<T>, val f: suspend (T) -> Boolean): RDD<T>(parent.master, parent.tClass) {
    override fun toImpl(): RDDImpl<T> {
        return FilteredRDDImpl(parent.toImpl(), f)
    }
}

class FilteredRDDImpl<T>(val parent: RDDImpl<T>, val f: suspend (T) -> Boolean): RDDImpl<T>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        val channel = Channel<T>(MAX_CAP)
        val recChannel = parent.channel(scope, ctx)
        scope.launch {
            (1..CONCURRENT_MAP_LIMIT).map {
                async {
                    for (t in recChannel) {
                        if (f(t))
                            channel.send(t)
                    }
                }
            }.awaitAll()
            channel.close()
        }
        return channel
    }
}
