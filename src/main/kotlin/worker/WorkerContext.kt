package worker

import shuffle.GrpcShuffleManager
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.*

class WorkerContext(
        val shuffleManagers: MutableMap<Int, GrpcShuffleManager<*>>,
        val cache: CacheManager) {
    val sampleRate: Double
    val masterAddress: String

    init {
        val properties = Properties()
        try {
            properties.load(FileInputStream("worker.properties"))
        }
        catch (e: FileNotFoundException) {

        }

        sampleRate = properties["sampleRate"]?.toString()?.toDouble() ?: 1.0
        masterAddress = properties["masterAddress"]?.toString() ?: "localhost:8080"
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