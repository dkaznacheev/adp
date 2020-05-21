package api.rdd

import worker.WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class FilteredRDD<T>(val parent: RDD<T>, val f: suspend (T) -> Boolean): RDD<T>(parent.master, parent.kryo) {
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
            val defs = mutableListOf<Deferred<*>>()
            while(!recChannel.isClosedForReceive) {
                val t = recChannel.receiveOrNull() ?: break
                defs.add(async {
                    if (f(t)) {
                        channel.send(t)
                    }
                })
            }
            defs.awaitAll()
            channel.close()
        }
        return channel
    }
}
