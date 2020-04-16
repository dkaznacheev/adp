package api.operations

import WorkerContext
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce

class ReduceOperation<T>(rdd: RDD<T>, val f: (T, T) -> T): ParallelOperation<T, T>(rdd) {
    override suspend fun consumeParts(channel: ReceiveChannel<T>): T {
        return channel.reduce(f)
    }

    override fun toImpl(): ParallelOperationImpl<T, T> {
        return ReduceOperationImpl(rdd.toImpl(), f)
    }
}

class ReduceOperationImpl<T>(rdd: RDDImpl<T>, val f: (T, T) -> T): ParallelOperationImpl<T, T>(rdd) {
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): T {
        return rdd.channel(scope,).reduce(f)
    }
}
