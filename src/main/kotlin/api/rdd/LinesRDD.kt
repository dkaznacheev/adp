package api.rdd


import com.esotericsoftware.kryo.Kryo
import master.Master
import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import utils.SerUtils
import java.io.File

class LinesRDD(master: Master,
                 private val filename: String): RDD<String>(master, Kryo()) {
    override fun toImpl(): RDDImpl<String> {
        return LinesRDDImpl(filename)
    }
}

class LinesRDDImpl(val filename: String): RDDImpl<String>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<String> {
        return scope.produce {
            withContext(Dispatchers.IO) {
                val file = File(filename)
                file.bufferedReader().useLines {
                    for (line in it) {
                        send(line)
                    }
                }
            }
        }
    }
}
