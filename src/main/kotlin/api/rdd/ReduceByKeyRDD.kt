package api.rdd

import WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

class ReduceByKeyRDD<K, T>(val parent: RDD<Pair<K, T>>, val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return ReduceByKeyRDDImpl(parent.toImpl(), f)
    }
}

class ReduceByKeyRDDImpl<K, T>(val parent: RDDImpl<Pair<K, T>>, val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Pair<K, T>> {
        val channel = Channel<Pair<K, T>>(MAX_CAP)
        val recChannel = parent.channel(scope, ctx)

        val table = ConcurrentHashMap<K, T>()
        scope.launch {
            for ((k, v) in recChannel) {

            }

            channel.close()
        }
        return channel
    }
}