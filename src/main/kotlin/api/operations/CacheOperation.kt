package api.operations

import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.reduce
import worker.WorkerContext

class CacheOperation<T>(rdd: RDD<T>,
                        val tClass: Class<T>,
                        val id: Int): ParallelOperation<T, Int>(rdd, Int::class.java) {
    @Suppress("DEPRECATION")
    override suspend fun consumeParts(channel: ReceiveChannel<Int>): Int {
        channel.reduce {_, _ -> SUCCESS.toInt() }
        return id
    }

    override fun toImpl(): ParallelOperationImpl<T, Int> {
        return CacheOperationImpl(
            rdd.toImpl(),
            tClass,
            id
        )
    }

    override val zero: Int
        get() = SUCCESS.toInt()
}

class CacheOperationImpl<T>(rdd: RDDImpl<T>, val tClass: Class<T>, val id: Int):
        ParallelOperationImpl<T, Int>(rdd, Int::class.java) {
    @ExperimentalCoroutinesApi
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Int {
        val recChannel = rdd.channel(scope, ctx)
        val cache = ctx.cache
        recChannel.consumeEach {
            cache.store(id, it)
        }
        cache.close()
        return SUCCESS.toInt()
    }
}
