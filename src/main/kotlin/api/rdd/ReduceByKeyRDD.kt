package api.rdd

import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
import master.LocalMaster
import master.MasterShuffleManager
import utils.SerUtils
import shuffle.GrpcShuffleManager
import shuffle.LocalShuffleManager
import shuffle.WorkerShuffleManager
import kotlin.Comparator
import kotlin.math.abs

fun <T> defaultComparatorFun() : (T, T) -> Int {
    return { a, b -> a.hashCode() - b.hashCode() }
}

fun <T> defaultComparator() : Comparator<T> {
    return kotlin.Comparator { a, b -> a.hashCode() - b.hashCode() }
}

fun <K, T> pairComparator(cmp: Comparator<K> = defaultComparator()): Comparator<Pair<K, T>> {
    return kotlin.Comparator { (k1, _), (k2, _) -> cmp.compare(k1, k2) }
}

class ReduceByKeyRDD<K, T>(val parent: RDD<Pair<K, T>>,
                           val keyComparator: (K, K) -> Int,
                           val serializer: SerUtils.Serializer<Pair<K, T>>,
                           val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    private val shuffleId = abs(hashCode())

    init {
        master.addShuffleManager<Pair<K, T>>(MasterShuffleManager(shuffleId, pairComparator<K, T>(kotlin.Comparator(keyComparator)), serializer))
    }

    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return master.getReduceByKeyRDDImpl(parent.toImpl(), shuffleId, keyComparator, serializer, f)
    }
}

abstract class ReduceByKeyRDDImpl<K, T>(val parent: RDDImpl<Pair<K, T>>,
                                        val shuffleId: Int,
                                        val keyComparator: (K, K) -> Int,
                                        val serializer: SerUtils.Serializer<Pair<K, T>>,
                                        val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {

    abstract fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<Pair<K, T>>

    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Pair<K, T>> {
        val recChannel = parent.channel(scope, ctx)
        val shuffleManager = getShuffleManager(ctx, shuffleId)

        return scope.produce<Pair<K, T>> {
            shuffleManager.writeAndBroadcast(scope, recChannel)
            val merged = shuffleManager.readMerged(scope)
            var currentPair: Pair<K, T>? = null

            for (pair in merged) {
                println(pair)
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

class ReduceByKeyGrpcRDDImpl<K, T>(parent: RDDImpl<Pair<K, T>>,
                                   shuffleId: Int,
                                   keyComparator: (K, K) -> Int,
                                   serializer: SerUtils.Serializer<Pair<K, T>>,
                                   f: (T, T) -> T):
        ReduceByKeyRDDImpl<K, T>(parent, shuffleId, keyComparator, serializer, f) {
    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<Pair<K, T>> {
        val manager = GrpcShuffleManager<Pair<K, T>>(ctx, shuffleId, pairComparator(kotlin.Comparator(keyComparator)), serializer)
        ctx.addShuffleManager(shuffleId, manager)
        return manager
    }

}


class LocalReduceByKeyRDDImpl<K, T>(parent: RDDImpl<Pair<K, T>>,
                                    shuffleId: Int,
                                    keyComparator: (K, K) -> Int,
                                    serializer: SerUtils.Serializer<Pair<K, T>>,
                                    f: (T, T) -> T):
        ReduceByKeyRDDImpl<K, T>(parent, shuffleId, keyComparator, serializer, f) {
    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<Pair<K, T>> {
        return LocalShuffleManager<Pair<K, T>>(ctx, shuffleId, pairComparator(kotlin.Comparator(keyComparator)), serializer)
    }
}
