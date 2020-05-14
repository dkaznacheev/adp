package api.rdd

import master.Master
import worker.WorkerContext
import api.operations.CacheOperation
import api.operations.ReduceOperation
import api.operations.SaveAsCsvOperation
import api.operations.SaveAsObjectOperation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import utils.SerUtils
import java.io.Serializable
import kotlin.math.abs

fun <K, V> RDD<Pair<K, V>>.reduceByKey(f: (V, V) -> V): RDD<Pair<K, V>> {
    return ReduceByKeyRDD(this, f)
}

inline fun <reified K, reified V> RDD<Pair<K, V>>.reduceByKeyV(noinline f: (V, V) -> V): RDD<Pair<K, V>> {
    val c = K::class.java
    return ReduceByKeyRDD(this, f)
}

abstract class RDD<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDD<R> {
        return MappedRDD(this, f)
    }

    fun <R> mapSync(f: suspend (T) -> R): RDD<R> {
        return MappedSyncRDD(this, f)
    }

    fun filter(f: suspend (T) -> Boolean): RDD<T> {
        return FilteredRDD(this, f)
    }

    fun reduce(f: (T, T) -> T): T? {
        return master.execute(ReduceOperation(this, f))
    }

    inline fun <reified T> saveAsObject(name: String) {
        master.execute(SaveAsObjectOperation(this, name, SerUtils.getSerializer(T::class)))
    }

    fun saveAsCSV(name: String) {
        master.execute(SaveAsCsvOperation(this, name))
    }

    fun show() {
        println(map { it.toString() }.reduce { a, b -> a + "\n" + b})
    }

    fun cache(): Int {
        val cacheId = abs(hashCode())
        return master.execute(CacheOperation(this, cacheId))
    }

    abstract fun toImpl(): RDDImpl<T>
}

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T>
}