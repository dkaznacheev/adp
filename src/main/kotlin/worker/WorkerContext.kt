package worker

class WorkerContext(
        val shuffleManager: ShuffleManager,
        val shuffleManagers: MutableMap<Int, GrpcShuffleManager<*>>,
        val cache: CacheManager) {

    fun addShuffleManager(shuffleId: Int, shuffleManager: GrpcShuffleManager<*>) {
        shuffleManagers[shuffleId] = shuffleManager
    }

    var workerId: String? = null

    companion object {
        fun stub(): WorkerContext {
            return WorkerContext(ShuffleManager(-1, listOf()),
            mutableMapOf(),
            CacheManager(100))
        }
    }
}