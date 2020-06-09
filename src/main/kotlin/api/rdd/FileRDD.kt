package api.rdd


import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import master.Master
import utils.KryoSerializer
import worker.WorkerContext
import java.io.File

class FileRDD<T>(master: Master,
                 private val filename: String,
                 tClass: Class<T>): RDD<T>(master, tClass) {
    override fun toImpl(): RDDImpl<T> {
        return FileRDDImpl(filename, tClass)
    }
}


inline fun <reified T> fileRdd(master: Master, filename: String): FileRDD<T> {
    return FileRDD(master, filename, T::class.java)
}

class FileRDDImpl<T>(val filename: String,
                     val tClass: Class<T>): RDDImpl<T>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        val serializer = KryoSerializer(tClass)
        return serializer.readFile(File(filename), scope)
    }
}