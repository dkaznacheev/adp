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
    val blockSize: Int
    val blockBufferSize: Int

    init {
        val properties = Properties()
        try {
            properties.load(FileInputStream("worker.properties"))
        }
        catch (e: FileNotFoundException) {

        }

        sampleRate = properties["sampleRate"]?.toString()?.toDouble() ?: 1.0
        masterAddress = properties["masterAddress"]?.toString() ?: "localhost:8080"
        blockSize = properties["blockSize"]?.toString()?.toInt() ?: 1000
        blockBufferSize = properties["blockBufferSize"]?.toString()?.toInt() ?: 100

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

/*

sampleRate=0.05
masterAddress=ec2-13-53-103-60.eu-north-1.compute.amazonaws.com:8099
blockSize=1000

 */