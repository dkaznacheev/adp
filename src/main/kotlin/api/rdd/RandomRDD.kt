package api.rdd


import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import master.Master
import worker.WorkerContext
import kotlin.random.Random

class RandomRDD(master: Master,
                 private val count: Int): RDD<Int>(master, Int::class.java) {
    override fun toImpl(): RDDImpl<Int> {
        return RandomRDDImpl(count)
    }
}

class RandomRDDImpl(val count: Int): RDDImpl<Int>() {
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<Int> {
        return scope.produce {
            val random = Random(System.currentTimeMillis())
            repeat(count) {
                send(random.nextInt())
            }
        }
    }
}
