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
import shuffle.LocalShuffleManager
//import shuffle.LocalShuffleManager
import shuffle.WorkerShuffleManager
import utils.NPair
import utils.toN
import kotlin.Comparator
import kotlin.math.abs


class SortedRDD<T>(val parent: RDD<T>,
                   val comparator: (T, T) -> Int): RDD<T>(parent.master, parent.tClass) {
    private val shuffleId = abs(hashCode())

    init {
        master.addShuffleManager<T>(MasterShuffleManager(shuffleId, kotlin.Comparator(comparator), tClass))
    }

    override fun toImpl(): RDDImpl<T> {
        return master.getSortedRDDImpl(parent.toImpl(), shuffleId, comparator, tClass)
    }
}

abstract class SortedRDDImpl<T>(val parent: RDDImpl<T>,
                                val shuffleId: Int,
                                val comparator: (T, T) -> Int,
                                val tClass: Class<T>): RDDImpl<T>() {

    abstract fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<T>

    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        val recChannel = parent.channel(scope, ctx)
        val shuffleManager = getShuffleManager(ctx, shuffleId)

        return scope.produce<T> {
            shuffleManager.writeAndBroadcast(scope, recChannel)
            System.err.println("finished writing")

            val merged = shuffleManager.readMerged(scope)
            for (t in merged) {
                send(t)
            }
        }
    }
}

class SortedGrpcRDDImpl<T>(parent: RDDImpl<T>,
                           shuffleId: Int,
                           comparator: (T, T) -> Int,
                           tClass: Class<T>):
        SortedRDDImpl<T>(parent, shuffleId, comparator, tClass) {
    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<T> {
        val manager = GrpcShuffleManager<T>(ctx, shuffleId, kotlin.Comparator(comparator), tClass)
        ctx.addShuffleManager(shuffleId, manager)
        return manager
    }
}


class LocalSortedRDDImpl<T>(parent: RDDImpl<T>,
                            shuffleId: Int,
                            keyComparator: (T, T) -> Int,
                            tClass: Class<T>):
        SortedRDDImpl<T>(parent, shuffleId, keyComparator, tClass) {
    override fun getShuffleManager(ctx: WorkerContext, shuffleId: Int): WorkerShuffleManager<T> {
        return LocalShuffleManager<T>(ctx, shuffleId, kotlin.Comparator(comparator), tClass)
    }
}
