package api.operations

import worker.WorkerContext
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import utils.KryoSerializer
import utils.SerUtils
import java.io.Serializable

abstract class ParallelOperation<T, R> (val rdd: RDD<T>, val rClass: Class<R>) {
    abstract fun toImpl(): ParallelOperationImpl<T, R>
    abstract val zero: R
    abstract suspend fun consumeParts(channel: ReceiveChannel<R>): R

    open fun serialize(): ByteArray {
        return SerUtils.serialize(toImpl())
    }
}

abstract class ParallelOperationImpl<T, R>(val rdd: RDDImpl<T>, val rClass: Class<R>): Serializable {
    abstract suspend fun execute(scope: CoroutineScope, ctx: WorkerContext) : R

    open suspend fun executeSerializable(scope: CoroutineScope, ctx: WorkerContext): ByteArray {
        val resultSerializer = KryoSerializer(rClass)
        val result = execute(scope, ctx)
        return resultSerializer.serialize(result)
    }
}