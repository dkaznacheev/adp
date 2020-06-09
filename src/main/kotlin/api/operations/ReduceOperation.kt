package api.operations

import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce
import worker.WorkerContext

fun <T> errorZero(): T {
    error("no zero in reduce")
}

class ReduceOperation<T>(rdd: RDD<T>,
                         tClass: Class<T>,
                         private val default: T,
                         val f: (T, T) -> T): ParallelOperation<T, T>(rdd, tClass) {
    @Suppress("DEPRECATION")
    override suspend fun consumeParts(channel: ReceiveChannel<T>): T {
        return try {
            channel.reduce(f)
        } catch (e: UnsupportedOperationException) {
            default
        }
    }

    override fun toImpl(): ParallelOperationImpl<T, T> {
        return ReduceOperationImpl(rdd.toImpl(), rClass, default, f)
    }

    override val zero: T
        get() = default
}

class ReduceOperationImpl<T>(rdd: RDDImpl<T>,
                             rClass: Class<T>,
                             val default: T,
                             val f: (T, T) -> T): ParallelOperationImpl<T, T>(rdd, rClass) {
    @Suppress("DEPRECATION")
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): T {
        return try {
            rdd.channel(scope, ctx).reduce(f)
        } catch (e: UnsupportedOperationException) {
            default
        }
    }
}
