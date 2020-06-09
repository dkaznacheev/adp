package api.rdd

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import master.Master
import worker.WorkerContext

@Suppress("UNUSED")
inline fun <reified T> cachedRDD(master: Master, cacheId: Int): CachedRDD<T> {
    return CachedRDD(master, T::class.java, cacheId)
}

class CachedRDD<T>(master: Master,
                   tClass: Class<T>,
                   val cacheId: Int): RDD<T>(master, tClass) {
    override fun toImpl(): RDDImpl<T> {
        return CachedRDDImpl(cacheId, tClass)
    }
}

class CachedRDDImpl<T>(val cacheId: Int,
                       val tClass: Class<T>): RDDImpl<T>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        return ctx.cache.load(cacheId, scope)
    }
}