package api.operations

import worker.WorkerContext
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import utils.SerUtils
import java.io.Serializable

abstract class ParallelOperation<T, R> (val rdd: RDD<T>) {
    abstract fun toImpl(): ParallelOperationImpl<T, R>
    abstract suspend fun consumeParts(channel: ReceiveChannel<R>): R

    open fun serialize(): ByteArray {
        return SerUtils.serialize(toImpl())
    }
}

abstract class ParallelOperationImpl<T, R>(val rdd: RDDImpl<T>, val serializer: SerUtils.Serializer<R>): Serializable {
    abstract suspend fun execute(scope: CoroutineScope, ctx: WorkerContext) : R

    open suspend fun executeSerializable(scope: CoroutineScope, ctx: WorkerContext): ByteArray {
        val result = execute(scope, ctx)
        return serializer.serialize(result)
    }
}