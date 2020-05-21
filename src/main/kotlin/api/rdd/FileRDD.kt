package api.rdd


import com.esotericsoftware.kryo.Kryo
import master.Master
import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import master.LocalMaster
import utils.SerUtils
import java.io.File

class FileRDD<T>(master: Master,
                 kryo: Kryo,
                 private val filename: String,
                 private val serializer: SerUtils.Serializer<T>): RDD<T>(master, kryo) {
    override fun toImpl(): RDDImpl<T> {
        return FileRDDImpl(filename, serializer)
    }
}

inline fun <reified T> fileRdd(master: Master, filename: String, kryo: Kryo = Kryo()): FileRDD<T> {
    return FileRDD(master, kryo, filename, SerUtils.kryoSerializer<T>(kryo))
}

class FileRDDImpl<T>(val filename: String, val serializer: SerUtils.Serializer<T>): RDDImpl<T>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        return serializer.readFile(File(filename), scope)
    }
}
