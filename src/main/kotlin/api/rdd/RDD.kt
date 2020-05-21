package api.rdd

import master.Master
import worker.WorkerContext
import api.operations.CacheOperation
import api.operations.ReduceOperation
import api.operations.SaveAsCsvOperation
import api.operations.SaveAsObjectOperation
import io.ktor.client.HttpClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import utils.SerUtils
import java.io.Serializable
import kotlin.math.abs

inline fun <reified K, reified V> RDD<Pair<K, V>>.reduceByKey(noinline comparator: (K, K) -> Int = defaultComparatorFun<K>(), noinline f: (V, V) -> V): RDD<Pair<K, V>> {
    val serializer = SerUtils.getPairSerializer<K, V>()
    return ReduceByKeyRDD(this, comparator, serializer, f)
}

inline fun <reified T> RDD<T>.saveAsObject(name: String) {
    master.execute(SaveAsObjectOperation(this, name, SerUtils.getSerializer(T::class)))
}

inline fun <reified T> RDD<T>.saveAsObject(serializer: SerUtils.Serializer<T>, name: String) {
    master.execute(SaveAsObjectOperation(this, name, serializer as SerUtils.Serializer<Any?>))
}

inline fun <reified T, reified R> RDD<T>.map(noinline f: suspend (T) -> R): RDD<R> {
    return MappedRDD(this, f)
}

inline fun <reified T> RDD<T>.show() {
    println(map { it.toString() }.reduce { a, b -> a + "\n" + b })
}

inline fun <reified T, reified R> RDD<T>.mapSync(noinline f: suspend (T) -> R): RDD<R> {
    return MappedSyncRDD(this, f)
}

inline fun <reified T, reified R> RDD<T>.mapHTTP(noinline f: suspend HttpClient.(T) -> R): RDD<R> {
    return HTTPMapRDD(this, f)
}

inline fun <reified T> RDD<T>.filter(noinline f: suspend (T) -> Boolean): RDD<T> {
    return FilteredRDD(this, f)
}

inline fun <reified T> RDD<T>.reduce(noinline f: (T, T) -> T): T? {
    return master.execute(ReduceOperation(this, f))
}

inline fun <reified T> RDD<T>.saveAsCSV(name: String) {
    master.execute(SaveAsCsvOperation(this, name))
}

inline fun <reified T> RDD<T>.cache(): Int {
    val cacheId = abs(hashCode())
    return master.execute(CacheOperation(this, cacheId))
}

abstract class RDD<T>(val master: Master) {
    abstract fun toImpl(): RDDImpl<T>
}

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T>
}