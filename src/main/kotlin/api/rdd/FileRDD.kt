package api.rdd


import master.Master
import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import master.LocalMaster
import utils.SerUtils
import java.io.File

class FileRDD<T>(master: Master,
                 private val filename: String,
                 private val serializer: SerUtils.Serializer<T>): RDD<T>(master) {
    override fun toImpl(): RDDImpl<T> {
        return FileRDDImpl(filename, serializer)
    }
}

inline fun <reified T> fileRdd(master: Master, filename: String): FileRDD<T> {
    return FileRDD(master, filename, SerUtils.getSerializer())
}

class FileRDDImpl<T>(val filename: String, val serializer: SerUtils.Serializer<T>): RDDImpl<T>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        val linesReader = File(filename).bufferedReader()
        return scope.produce {
            for (line in linesReader.lines()) {
                send(serializer.deserialize(line))
            }
        }
    }
}
