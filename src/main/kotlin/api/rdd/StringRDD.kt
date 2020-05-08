package api.rdd

import master.Master
import worker.WorkerContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import java.io.File

class StringRDD(master: Master, val filename: String) : RDD<String>(master) {
    override fun toImpl(): RDDImpl<String> {
        return StringRDDImpl(filename)
    }
}

@ExperimentalCoroutinesApi
class StringRDDImpl(val filename: String): RDDImpl<String>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<String> {
        val lines = File(filename).bufferedReader().lineSequence()
        return scope.produce {
            for (line in lines) {
                send(line)
            }
        }
    }
}
