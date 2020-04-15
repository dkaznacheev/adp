package api.rdd

import Master
import api.operations.ReduceOperation
import api.operations.SaveAsObjectOperation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import java.io.Serializable

abstract class RDD<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDD<R> {
        return MappedRDD(this, f)
    }

    fun reduce(f: (T, T) -> T): T? {
        return master.execute(ReduceOperation(this, f))
    }

    fun saveAsObject(name: String) {
        master.execute(SaveAsObjectOperation(this, name))
    }

    abstract fun toImpl(): RDDImpl<T>
}

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}