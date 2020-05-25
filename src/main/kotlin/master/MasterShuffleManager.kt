package master

import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import utils.KryoSerializer
import utils.SerUtils
import utils.kryoSerializer

class MasterShuffleManager<T>(val shuffleId: Int,
                              private val comparator: Comparator<T>,
                              private val tClass: Class<T>) {
    private val distributionChannel = Channel<Adp.WorkerDistribution>(10000)
    private var distribution: Deferred<List<ByteString>>? = null

    fun listenForDistributions(scope: CoroutineScope,
                               workers: List<String>) {
        scope.launch {
            val serializer = KryoSerializer(tClass)
            distribution = async {
                val workersRemaining = workers.toMutableList()
                val distributions = mutableListOf<T>()

                while (workersRemaining.isNotEmpty()) {
                    val dst = distributionChannel.receive()
                    workersRemaining.remove(dst.workerId)
                    val sample = dst.sampleList.map { serializer.deserialize(it.toByteArray()) }
                    distributions.addAll(sample)
                }

                distributions.sortWith(comparator)
                val rangeSize = distributions.size / (workers.size)
                val finalDistribution = distributions
                        .filterIndexed { i, _ -> i % rangeSize == 1 }
                        .take(workers.size - 1)
                        .toList()
                finalDistribution.map { ByteString.copyFrom(serializer.serialize(it)) }
            }
        }
    }

    suspend fun sampleDistribution(workerDist: Adp.WorkerDistribution, workers: List<String>): Adp.Distribution {
        distributionChannel.send(workerDist)
        val partitions = distribution?.await() ?: return Adp.Distribution.newBuilder().build()
        val index = workers.indexOf(workerDist.workerId)
        return Adp.Distribution.newBuilder()
                .addAllPartitions(partitions)
                .setMyPartitionId(index)
                .addAllWorkers(workers)
                .build()
    }
}