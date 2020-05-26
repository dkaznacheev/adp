package api.rdd

import api.operations.CacheOperation
import master.Master
import worker.WorkerContext
//import api.operations.CacheOperation
import api.operations.ReduceOperation
import api.operations.SaveAsObjectOperation
import api.operations.errorZero
//import api.operations.SaveAsCsvOperation
//import api.operations.SaveAsObjectOperation
import com.esotericsoftware.kryo.Kryo
import io.ktor.client.HttpClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import utils.NPair
import utils.SerUtils
import java.io.Serializable
import kotlin.math.abs

inline fun <reified K, reified V> RDD<NPair<K, V>>.reduceByKey(noinline comparator: (K, K) -> Int = defaultComparatorFun<K>(), noinline f: (V, V) -> V): RDD<NPair<K, V>> {
    return ReduceByKeyRDD(this, comparator, f)
}

inline fun <reified T> RDD<T>.sorted(noinline comparator: (T, T) -> Int = defaultComparatorFun<T>()): RDD<T> {
    return SortedRDD(this, comparator)
}

inline fun <reified T> RDD<T>.saveAsObject(name: String) {
    master.execute(SaveAsObjectOperation(this, name, T::class.java))
}


inline fun <reified T, reified R> RDD<T>.map(noinline f: suspend (T) -> R): RDD<R> {
    return MappedRDD(this, R::class.java, f)
}

inline fun <reified T> RDD<T>.show() {
    println(map { it.toString() }.reduce("") { a, b -> a + "\n" + b })
}

inline fun <reified T, reified R> RDD<T>.mapSync(noinline f: suspend (T) -> R): RDD<R> {
    return MappedSyncRDD(this, R::class.java, f)
}

inline fun <reified T, reified R> RDD<T>.mapHTTP(noinline f: suspend HttpClient.(T) -> R): RDD<R> {
    return HTTPMapRDD(this, R::class.java, f)
}

inline fun <reified T> RDD<T>.filter(noinline f: suspend (T) -> Boolean): RDD<T> {
    return FilteredRDD(this, f)
}

inline fun <reified T> RDD<T>.reduce(default: T = errorZero(), noinline f: (T, T) -> T): T? {
    return master.execute(ReduceOperation(this, T::class.java, default, f))
}

inline fun <reified T> RDD<T>.cache(): Int {
    val cacheId = abs(hashCode())
    return master.execute(CacheOperation(this, T::class.java, cacheId))
}

abstract class RDD<T>(val master: Master, val tClass: Class<T>) {
    abstract fun toImpl(): RDDImpl<T>
}

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T>
}