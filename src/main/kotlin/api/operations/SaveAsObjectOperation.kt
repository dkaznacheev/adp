package api.operations

import WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.cio.writeChannel
import io.ktor.utils.io.ByteWriteChannel
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
        val recChannel = rdd.channel(scope, ctx)
        val outFile = File(name)
        writeToFile(scope, recChannel, outFile)
        return SUCCESS
    }

    private suspend fun writeToFile(scope: CoroutineScope, recChannel: ReceiveChannel<T>, outFile: File) {
        withContext(Dispatchers.IO) {
            var initialized = false
            var serializer: SerUtils.Serializer<Any?>? = null
            val writer = outFile.bufferedWriter()
            recChannel.consumeEach { o ->
                if (!initialized) {
                    serializer = SerUtils.getSerializer(o as Any)
                    initialized = false
                }
                serializer?.serialize(o)?.let { writer.write(it) }
            }
        }
    }
}
