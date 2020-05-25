package api.operations

import worker.WorkerContext
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce
import utils.SerUtils

class ReduceOperation<T>(rdd: RDD<T>,
                         tClass: Class<T>,
                         val f: (T, T) -> T): ParallelOperation<T, T>(rdd, tClass) {
    override suspend fun consumeParts(channel: ReceiveChannel<T>): T {
        return channel.reduce(f)
    }

    override fun toImpl(): ParallelOperationImpl<T, T> {
        return ReduceOperationImpl(rdd.toImpl(), rClass, f)
    }
}

class ReduceOperationImpl<T>(rdd: RDDImpl<T>,
                             rClass: Class<T>,
                             val f: (T, T) -> T): ParallelOperationImpl<T, T>(rdd, rClass) {
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): T {
        return rdd.channel(scope, ctx).reduce(f)
    }
}
