package api.rdd

import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
//import master.LocalMaster
import master.MasterShuffleManager
import utils.SerUtils
import shuffle.GrpcShuffleManager
//import shuffle.LocalShuffleManager
import shuffle.WorkerShuffleManager
import utils.NPair
import utils.toN
import kotlin.Comparator
import kotlin.math.abs


fun <T> defaultComparatorFun() : (T, T) -> Int {
    return { a, b -> a.hashCode() - b.hashCode() }
}

fun <T> defaultComparator() : Comparator<T> {
    return kotlin.Comparator { a, b -> a.hashCode() - b.hashCode() }
}

fun <K, T> pairComparator(cmp: Comparator<K> = defaultComparator()): Comparator<NPair<K, T>> {
    return kotlin.Comparator { n1, n2 -> cmp.compare(n1.first, n2.first) }
}

class ReduceByKeyRDD<K, T>(val parent: RDD<NPair<K, T>>,
                           val keyComparator: (K, K) -> Int,
                           val f: (T, T) -> T): RDD<NPair<K, T>>(parent.master, NPair::class.java as Class<NPair<K, T>>) {
    private val shuffleId = abs(hashCode())

    init {
        master.addShuffleManager<NPair<K, T>>(MasterShuffleManager(shuffleId, pairComparator<K, T>(kotlin.Comparator(keyComparator)), tClass))
    }

    override fun toImpl(): RDDImpl<NPair<K, T>> {
        return master.getReduceByKeyRDDImpl(parent.toImpl(), shuffleId, keyComparator, tClass, f)
    }
}

abstract class ReduceByKeyRDDImpl<K, T>(val parent: RDDImpl<NPair<K, T>>,
                                        val shuffleId: Int,
                                        val keyComparator: (K, K) -> Int,
                                        val tClass: Class<NPair<K, T>>,
                                        val f: (T, T) -> T): RDDImpl<NPair<K, T>>() {

    abstract fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<NPair<K, T>>

    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<NPair<K, T>> {
        val recChannel = parent.channel(scope, ctx)
        val shuffleManager = getShuffleManager(ctx, shuffleId)

        return scope.produce<NPair<K, T>> {
            shuffleManager.writeAndBroadcast(scope, recChannel)
            System.err.println("finished writing")

            val merged = shuffleManager.readMerged(scope)
            var currentPair: NPair<K, T>? = null

            for (pair in merged) {
                currentPair = if (currentPair == null) {
                    pair
                } else {
                    if (currentPair.first != pair.first) {
                        send(currentPair)
                        pair
                    } else {
                        pair.first toN f(currentPair.second, pair.second)
                    }
                }
            }

            if (currentPair != null) {
                send(currentPair)
            }
        }
    }
}

class ReduceByKeyGrpcRDDImpl<K, T>(parent: RDDImpl<NPair<K, T>>,
                                   shuffleId: Int,
                                   keyComparator: (K, K) -> Int,
                                   tClass: Class<NPair<K, T>>,
                                   f: (T, T) -> T):
        ReduceByKeyRDDImpl<K, T>(parent, shuffleId, keyComparator, tClass, f) {
    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<NPair<K, T>> {
        val manager = GrpcShuffleManager<NPair<K, T>>(ctx, shuffleId, pairComparator(kotlin.Comparator(keyComparator)), tClass)
        ctx.addShuffleManager(shuffleId, manager)
        return manager
    }

}

//
//class LocalReduceByKeyRDDImpl<K, T>(parent: RDDImpl<Pair<K, T>>,
//                                    shuffleId: Int,
//                                    keyComparator: (K, K) -> Int,
//                                    serializer: SerUtils.Serializer<Pair<K, T>>,
//                                    f: (T, T) -> T):
//        ReduceByKeyRDDImpl<K, T>(parent, shuffleId, keyComparator, serializer, f) {
//    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<Pair<K, T>> {
//        return LocalShuffleManager<Pair<K, T>>(ctx, shuffleId, pairComparator(kotlin.Comparator(keyComparator)), serializer)
//    }
//}
