package api.operations

import worker.WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.reduce
import utils.SerUtils

class CacheOperation<T>(rdd: RDD<T>,
                        val serializer: SerUtils.Serializer<T>,
                        val id: Int): ParallelOperation<T, Int>(rdd) {
    override suspend fun consumeParts(channel: ReceiveChannel<Int>): Int {
        channel.reduce {_, _ -> SUCCESS.toInt() }
        return id
    }

    override fun toImpl(): ParallelOperationImpl<T, Int> {
        return CacheOperationImpl(
            rdd.toImpl(),
            serializer,
            id
        )
    }
}

class CacheOperationImpl<T>(rdd: RDDImpl<T>, val spillSerializer: SerUtils.Serializer<T>, val id: Int):
        ParallelOperationImpl<T, Int>(rdd, SerUtils.kryoSerializer<Int>()) {
    @KtorExperimentalAPI
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
