package api.rdd

import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
import master.MasterShuffleManager
import utils.SerUtils
import worker.GrpcShuffleManager
import kotlin.Comparator
import kotlin.math.abs

fun <T> defaultComparator() : Comparator<T> {
    return kotlin.Comparator { a, b -> a.hashCode() - b.hashCode() }
}

fun <K, T> pairComparator(cmp: Comparator<K> = defaultComparator()): Comparator<Pair<K, T>> {
    return kotlin.Comparator { (k1, _), (k2, _) -> cmp.compare(k1, k2) }
}

class ReduceByKeyGrpcRDD<K, T>(val parent: RDD<Pair<K, T>>,
                               val keyComparator: Comparator<K> = defaultComparator(),
                               val serializer: SerUtils.Serializer<Pair<K, T>>,
                               val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    private val shuffleId = abs(hashCode())

    init {
        master.addShuffleManager(MasterShuffleManager(shuffleId, pairComparator(keyComparator), serializer))
    }

    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return ReduceByKeyGrpcRDDImpl(parent.toImpl(), shuffleId, keyComparator, serializer, f)
    }
}

class ReduceByKeyGrpcRDDImpl<K, T>(val parent: RDDImpl<Pair<K, T>>,
                                   val shuffleId: Int,
                                   val comparator: Comparator<K>,
                                   val serializer: SerUtils.Serializer<Pair<K, T>>,
                                   val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Pair<K, T>> {
        val recChannel = parent.channel(scope, ctx)

        val shuffleManager = GrpcShuffleManager(ctx, shuffleId, pairComparator(comparator), serializer)
        ctx.addShuffleManager(shuffleId, shuffleManager)

        scope.launch {
            shuffleManager.writeAndBroadcast(scope, recChannel)
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