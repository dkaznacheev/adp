package api.rdd

import Master
import api.operations.ReduceByKeyOperation
import api.operations.ReduceOperation
import api.operations.SaveAsCsvOperation
import api.operations.SaveAsObjectOperation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import java.io.Serializable

fun <K, V> RDD<Pair<K, V>>.reduceByKey(f: (V, V) -> V): RDD<Pair<K, V>> {
    return ReduceByKeyOperation(this, f)
}

abstract class RDD<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDD<R> {
        return MappedRDD(this, f)
    }

    fun filter(f: suspend (T) -> Boolean): RDD<T> {
        return FilteredRDD(this, f)
    }

    fun reduce(f: (T, T) -> T): T? {
        return master.execute(ReduceOperation(this, f))
    }

    fun saveAsObject(name: String) {
        master.execute(SaveAsObjectOperation(this, name))
    }

    fun saveAsCSV(name: String) {
        master.execute(SaveAsCsvOperation(this, name))
    }

    abstract fun toImpl(): RDDImpl<T>
}

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}