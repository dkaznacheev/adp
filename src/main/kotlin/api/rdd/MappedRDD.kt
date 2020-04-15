package api.rdd

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
    override fun channel(scope: CoroutineScope): ReceiveChannel<R> {
        val channel = Channel<R>(MAX_CAP)
        val recChannel = parent.channel(scope)
        scope.launch {
            val defs = mutableListOf<Deferred<*>>()
            while(!recChannel.isClosedForReceive) {
                val t = recChannel.receiveOrNull() ?: break
                defs.add(async {
                    channel.send(f(t))
                })
            }
            defs.awaitAll()
            channel.close()
        }
        return channel
    }
}
