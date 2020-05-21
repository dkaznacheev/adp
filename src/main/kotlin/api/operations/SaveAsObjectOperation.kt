package api.operations

import worker.WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.reduce
import kotlinx.coroutines.withContext
import utils.SerUtils
import java.io.File

class SaveAsObjectOperation<T>(rdd: RDD<T>,
                               val name: String,
                               val serializer: SerUtils.Serializer<Any?>): ParallelOperation<T, Byte>(rdd) {
    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsObjectOperationImpl(
            rdd.toImpl(),
            name,
            serializer
        )
    }

    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }
}

class SaveAsObjectOperationImpl<T>(rdd: RDDImpl<T>,
                                   val name: String,
                                   val serializer: SerUtils.Serializer<Any?>): ParallelOperationImpl<T, Byte>(rdd) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Byte {
        val recChannel = rdd.channel(scope, ctx)
        val outFile = File(name)
        writeToFile(scope, recChannel, outFile)
        return SUCCESS
    }

    private suspend fun writeToFile(scope: CoroutineScope, recChannel: ReceiveChannel<T>, outFile: File) {
        serializer.writeToFile(scope, recChannel, outFile)
//        withContext(Dispatchers.IO) {
//            val writer = outFile.bufferedWriter()
//            recChannel.consumeEach { o ->
//                writer.write(serializer.serialize(o))
//                writer.newLine()
//            }
//            writer.flush()
//            writer.close()
//        }
    }
}
