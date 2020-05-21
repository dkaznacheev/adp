package api.rdd

import com.esotericsoftware.kryo.Kryo
import master.Master
import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel

class CachedRDD<T>(master: Master,
                   kryo: Kryo,
                   val cacheId: Int): RDD<T>(master, kryo) {
    override fun toImpl(): RDDImpl<T> {
        return CachedRDDImpl(cacheId)
    }
}

class CachedRDDImpl<T>(val cacheId: Int): RDDImpl<T>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        return ctx.cache.load(cacheId, scope)
    }
}