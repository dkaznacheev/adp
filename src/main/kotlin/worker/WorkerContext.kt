package worker

import shuffle.GrpcShuffleManager
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.*

class WorkerContext(
        val shuffleManagers: MutableMap<Int, GrpcShuffleManager<*>>,
        val cache: CacheManager) {
    val masterAddress: String
    val blockSize: Int
    val blockBufferSize: Int

    init {
        val properties = Properties()
        try {
            properties.load(FileInputStream("worker.properties"))
        }
        catch (e: FileNotFoundException) {

        }

        masterAddress = properties["masterAddress"]?.toString() ?: "localhost:8080"
        blockSize = properties["blockSize"]?.toString()?.toInt() ?: 1000
        blockBufferSize = properties["blockBufferSize"]?.toString()?.toInt() ?: 100

    }

    fun addShuffleManager(shuffleId: Int, shuffleManager: GrpcShuffleManager<*>) {
        shuffleManagers[shuffleId] = shuffleManager
    }

    var workerId: String? = null

    companion object {
        @Suppress("UNUSED")
        fun stub(): WorkerContext {
            return WorkerContext(
            mutableMapOf(),
            CacheManager(100))
        }
    }
}