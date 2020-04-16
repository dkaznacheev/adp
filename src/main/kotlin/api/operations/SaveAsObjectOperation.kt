package api.operations

import WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.cio.writeChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.reduce
import kotlinx.coroutines.withContext
import utils.SerUtils
import java.io.File

class SaveAsObjectOperation<T>(rdd: RDD<T>, val name: String): ParallelOperation<T, Byte>(rdd) {
    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsCsvOperationImpl(
            rdd.toImpl(),
            name
        )
    }

    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }
}

class SaveAsObjectOperationImpl<T>(rdd: RDDImpl<T>, val name: String): ParallelOperationImpl<T, Byte>(rdd) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Byte {
        val recChannel = rdd.channel(scope,)
        val outChannel = File(name).writeChannel()
        return withContext(Dispatchers.IO) {
            recChannel.consumeEach {
                val ba = SerUtils.serialize(it)
                outChannel.writeFully(ba, 0, ba.size)
            }
            SUCCESS
        }
    }
}
