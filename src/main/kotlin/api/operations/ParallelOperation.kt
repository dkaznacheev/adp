package api.operations

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

abstract class ParallelOperationImpl<T, R>(val rdd: RDDImpl<T>): Serializable {
    abstract suspend fun execute(scope: CoroutineScope) : R

    open suspend fun executeSerializable(scope: CoroutineScope): ByteArray {
        val result = execute(scope) as Serializable
        return SerUtils.serialize(result)
    }
}