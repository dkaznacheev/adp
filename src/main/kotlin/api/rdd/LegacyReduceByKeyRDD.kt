package api.rdd

import worker.WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

class LegacyReduceByKeyRDD<K, T>(val parent: RDD<Pair<K, T>>, val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return LegacyReduceByKeyRDDImpl(parent.toImpl(), f)
    }
}

class LegacyReduceByKeyRDDImpl<K, T>(val parent: RDDImpl<Pair<K, T>>, val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Pair<K, T>> {
        val channel = Channel<Pair<K, T>>(MAX_CAP)
        val recChannel = parent.channel(scope, ctx)

        val shuffleManager = ctx.shuffleManager

        scope.launch {
            shuffleManager.createBlocks(recChannel)
            shuffleManager.startBroadcast()

            val partitionChannel = shuffleManager.loadBlocks<Pair<K, T>>(this)
            val table = ConcurrentHashMap<K, T>()

            for ((k, v) in partitionChannel) {
                if (!table.containsKey(k)) {
                    table[k] = v
                } else {
                    table[k] = f(table[k]!!, v)
                }
            }
            for (element in table.entries) {
                channel.send(element.key to element.value)
            }

            channel.close()
        }
        return channel
    }
}