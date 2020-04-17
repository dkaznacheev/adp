package api.rdd

import Master
import WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel

class CachedRDD<T>(master: Master,
             val cacheId: Int): RDD<T>(master) {
    override fun toImpl(): RDDImpl<T> {
        return CachedRDDImpl(cacheId)
    }
}

class CachedRDDImpl<T>(val cacheId: Int): RDDImpl<T>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        return ctx.cache.load(cacheId, scope)
    }
}