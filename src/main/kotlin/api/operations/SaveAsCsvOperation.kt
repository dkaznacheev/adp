package api.operations

import worker.WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.cio.writeChannel
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.reduce
import kotlinx.coroutines.withContext
import java.io.File

class SaveAsCsvOperation<T>(rdd: RDD<T>, val name: String): ParallelOperation<T, Byte>(rdd) {
    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }

    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsCsvOperationImpl(
            rdd.toImpl(),
            name
        )
    }
}

class SaveAsCsvOperationImpl<T>(rdd: RDDImpl<T>, val name: String): ParallelOperationImpl<T, Byte>(rdd) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Byte {
        val recChannel = rdd.channel(scope, ctx)
        val outChannel = File(name).writeChannel()
        return withContext(Dispatchers.IO) {
            recChannel.consumeEach {
                outChannel.writeStringUtf8(it.toString())
                outChannel.writeStringUtf8("\n")
            }
            SUCCESS
        }
    }
}
