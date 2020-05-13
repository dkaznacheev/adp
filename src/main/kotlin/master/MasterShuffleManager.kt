package master

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import utils.SerUtils

class MasterShuffleManager<T>(val shuffleId: Int, private val comparator: Comparator<T>) {
    private val distributionChannel = Channel<Adp.WorkerDistribution>(10000)

    fun listenForDistributions(scope: CoroutineScope,
                               workers: List<String>) {
        scope.launch {
            val workersRemaining = workers.toMutableList()
            val distributions = mutableListOf<T>()

            while(workersRemaining.isNotEmpty()) {
                val dst = distributionChannel.receive()
                workersRemaining.remove(dst.workerId)
                val sample = dst.sampleList.map { SerUtils.deserialize(it.toByteArray()) as T }
                distributions.addAll(sample)
            }

            distributions.sortWith(comparator)
        }
    }

    suspend fun sampleDistribution(workerDist: Adp.WorkerDistribution): Adp.Distribution {
        return Adp.Distribution.newBuilder().build()
    }
}