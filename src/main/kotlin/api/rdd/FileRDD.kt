package api.rdd


import com.esotericsoftware.kryo.Kryo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import master.Master
import utils.KryoSerializer
import worker.WorkerContext
import java.io.File

class FileRDD<T>(master: Master,
                 kryo: Kryo,
                 private val filename: String,
                 tClass: Class<T>): RDD<T>(master, tClass) {
    override fun toImpl(): RDDImpl<T> {
        return FileRDDImpl(filename, tClass)
    }
}


inline fun <reified T> fileRdd(master: Master, filename: String, kryo: Kryo = Kryo()): FileRDD<T> {
    return FileRDD(master, kryo, filename, T::class.java)
}

class FileRDDImpl<T>(val filename: String,
                     val tClass: Class<T>): RDDImpl<T>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<T> {
        val serializer = KryoSerializer(tClass)
        return serializer.readFile(File(filename), scope)
    }
}