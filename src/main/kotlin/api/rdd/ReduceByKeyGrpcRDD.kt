package api.rdd

import WorkerContext
import api.MAX_CAP
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch

class ReduceByKeyGrpcRDD<K, T>(val parent: RDD<Pair<K, T>>, val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    private val shuffleId = hashCode()

    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return ReduceByKeyGrpcRDDImpl(parent.toImpl(), shuffleId, f)
    }
}

class ReduceByKeyGrpcRDDImpl<K, T>(val parent: RDDImpl<Pair<K, T>>,
                                   val shuffleId: Int,
                                   val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Pair<K, T>> {
        val recChannel = parent.channel(scope, ctx)
        val shuffleManager = ctx.grpcShuffleManager
        scope.launch {
            shuffleManager.writeAndBroadcast(scope, recChannel, shuffleId)
        }

        return scope.produce<Pair<K, T>> {
            val merged = shuffleManager.readMerged<Pair<K, T>>(scope, shuffleId)
            var currentPair: Pair<K, T>? = null

            for (pair in merged) {
                if (currentPair == null) {
                    currentPair = pair
                }

                if (currentPair.first != pair.first) {
                    send(currentPair)
                    currentPair = pair
                } else {
                    currentPair = pair.first to f(currentPair.second, pair.second)
                }
            }

            if (currentPair != null) {
                send(currentPair)
            }
        }
    }
}