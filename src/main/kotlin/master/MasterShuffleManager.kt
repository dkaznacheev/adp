package master

import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import utils.*

class MasterShuffleManager<T>(val shuffleId: Int,
                              private val comparator: Comparator<T>,
                              private val tClass: Class<T>) {
    private val distributionChannel = Channel<Adp.WorkerDistribution>(10000)
    private var distribution: Deferred<List<ByteString>>? = null

    private fun getDistibutionParts(sample: List<T>, parts: Int): List<T> {
        var t: T? = null
        val unique = sample.filter {
            if (t == null) {
                t = it
                true
            } else {
                if (comparator.compare(t, it) == 0) {
                    false
                } else {
                    t = it
                    true
                }
            }
        }

        val rangeSize = unique.size / (parts)
        return unique.filterIndexed { i, _ -> i % rangeSize == 0 }
                .take(parts)
                .drop(1)
                .toList()
    }

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
                    println("sample $sample")
                    distributions.addAll(sample)
                }

                distributions.sortWith(comparator)
                println("Distributions: $distributions")
                val finalDistribution = getDistibutionParts(distributions, workers.size)
                println(finalDistribution)
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