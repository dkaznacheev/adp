package api.operations

import worker.WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce
import utils.SerUtils
import java.io.File

class SaveAsObjectOperation<T>(rdd: RDD<T>,
                               val name: String,
                               val serializer: SerUtils.Serializer<T>): ParallelOperation<T, Byte>(rdd) {
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
                                   val objectSerializer: SerUtils.Serializer<T>):
        ParallelOperationImpl<T, Byte>(rdd, SerUtils.kryoSerializer<Byte>()) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Byte {
        val recChannel = rdd.channel(scope, ctx)
        val outFile = File(name)
        writeToFile(scope, recChannel, outFile)
        return SUCCESS
    }

    private suspend fun writeToFile(scope: CoroutineScope, recChannel: ReceiveChannel<T>, outFile: File) {
        objectSerializer.writeToFile(recChannel, outFile)
    }
}
