package master

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import utils.SerUtils

class MasterShuffleManager<T>(val shuffleId: Int, val comparator: Comparator<T>) {
    private val distributionChannel = Channel<Adp.WorkerDistribution>(10000)

    fun listenForDistributions(scope: CoroutineScope,
                               workers: List<String>) {
        scope.launch {
            val workersRemaining = workers.toMutableList()
            val distributions = mutableListOf<T>()

            while(workersRemaining.isNotEmpty()) {
                val dst = distributionChannel.receive()
                workersRemaining.remove(dst.workerId)
                val dMin: T = SerUtils.unwrap(dst.min) as T
                val dMax: T = SerUtils.unwrap(dst.max) as T
                distributions.add(dMin)
                distributions.add(dMax)
            }

            distributions.sortWith(comparator)

        }
    }

    suspend fun sampleDistribution(workerDist: Adp.WorkerDistribution): Adp.Distribution {
        return Adp.Distribution.newBuilder().build()
    }
}