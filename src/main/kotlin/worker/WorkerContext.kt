package worker

import shuffle.GrpcShuffleManager
import java.io.FileInputStream
import java.util.*

class WorkerContext(
        val shuffleManagers: MutableMap<Int, GrpcShuffleManager<*>>,
        val cache: CacheManager) {

    val properties = Properties()
    val sampleRate: Double
    val masterAddress: String

    init {
        properties.load(FileInputStream("worker.properties"))
        sampleRate = properties["sampleRate"]?.toString()?.toDouble() ?: 1.0
        masterAddress = properties["masterAddress"]?.toString() ?: "localhost:8080"
        println(sampleRate)
        println(masterAddress)
    }

    fun addShuffleManager(shuffleId: Int, shuffleManager: GrpcShuffleManager<*>) {
        shuffleManagers[shuffleId] = shuffleManager
    }

    var workerId: String? = null

    companion object {
        fun stub(): WorkerContext {
            return WorkerContext(
            mutableMapOf(),
            CacheManager(100))
        }
    }
}