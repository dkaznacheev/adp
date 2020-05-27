package api.operations

import worker.WorkerContext
import api.SUCCESS
import api.rdd.RDD
import api.rdd.RDDImpl
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce
import kotlinx.coroutines.withContext
import utils.KryoSerializer
import java.io.File

class SaveAsTextOperation<T>(rdd: RDD<T>,
                               val name: String,
                               tClass: Class<T>): ParallelOperation<T, Byte>(rdd, Byte::class.java) {
    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsTextOperationImpl(
            rdd.toImpl(),
            name,
            rdd.tClass
        )
    }

    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }

    override val zero: Byte
        get() = SUCCESS
}

class SaveAsTextOperationImpl<T>(rdd: RDDImpl<T>,
                                   val name: String,
                                   val tClass: Class<T>):
        ParallelOperationImpl<T, Byte>(rdd, Byte::class.java) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope, ctx: WorkerContext): Byte {
        val recChannel = rdd.channel(scope, ctx)
        val outFile = File(name)
        withContext(Dispatchers.IO) {
            outFile.bufferedWriter().use {
                for (v in recChannel) {
                    it.write(v.toString())
                    it.newLine()
                }
            }
        }
        return SUCCESS
    }
}
