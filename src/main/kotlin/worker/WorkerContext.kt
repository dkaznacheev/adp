package worker

import shuffle.GrpcShuffleManager
import shuffle.LegacyHashShuffleManager

class WorkerContext(
        val shuffleManager: LegacyHashShuffleManager,
        val shuffleManagers: MutableMap<Int, GrpcShuffleManager<*>>,
        val cache: CacheManager) {

    fun addShuffleManager(shuffleId: Int, shuffleManager: GrpcShuffleManager<*>) {
        shuffleManagers[shuffleId] = shuffleManager
    }

    var workerId: String? = null

    companion object {
        fun stub(): WorkerContext {
            return WorkerContext(LegacyHashShuffleManager(-1, listOf()),
            mutableMapOf(),
            CacheManager(100))
        }
    }
}