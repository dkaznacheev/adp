package shuffle

import Adp
import MasterGrpcKt
import api.rdd.pairComparator
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import utils.ExternalSorter
import utils.LazyChannel
import utils.SerUtils
import worker.WorkerContext
import java.io.File
import java.util.*
import kotlin.random.Random
import kotlin.streams.toList
import kotlin.system.measureTimeMillis

class GrpcShuffleManager<T>(val ctx: WorkerContext,
                            private val shuffleId: Int,
                            private val comparator: Comparator<T>,
                            private val serializer: SerUtils.Serializer<T>): WorkerShuffleManager<T> {
    private val masterAddress = "localhost:8099"
    private val outPath = File("shuffle/outg")
    private val shuffleDir = outPath.resolve("shuffle$shuffleId")

    private val SAMPLE_RATE = 1.0

    private val masterStub = MasterGrpcKt.MasterCoroutineStub(ManagedChannelBuilder.forTarget(masterAddress)
            .usePlaintext()
            .build())

    private val partitionId = LazyChannel<Int>()
    private val blocks = LazyChannel<List<LazyChannel<File>>>()
    private val stubs = LazyChannel<List<WorkerGrpcKt.WorkerCoroutineStub>>()

    fun blockFor(workerNum: Int): Flow<Adp.Value> {
        return flow {
            val file = blocks.get()[workerNum].get()
            for (line in file.bufferedReader().lines()) {
                emit(Adp.Value.newBuilder().setValue(ByteString.copyFrom(line.toByteArray())).build())
            }
        }
    }

    override suspend fun writeAndBroadcast(
            scope: CoroutineScope,
            recChannel: ReceiveChannel<T>) {
        if (!shuffleDir.exists()) {
            shuffleDir.mkdir()
        }

        ExternalSorter(shuffleDir, comparator, serializer).sortAndWrite(scope, recChannel)
        val sample = getSample(shuffleDir.resolve("block"))

        val request = Adp.WorkerDistribution.newBuilder()
                .addAllSample(sample)
                .setWorkerId(ctx.workerId ?: error("Null workerId"))
                .setShuffleId(shuffleId)
                .build()

        System.err.println("awaiting distribution")
        val distribution = masterStub.sampleDistribution(request)

        partitionId.set(distribution.myPartitionId)
        blocks.set((0 until distribution.partitionsList.size).map { LazyChannel<File>() })
        stubs.set(distribution.workersList.map {
            WorkerGrpcKt.WorkerCoroutineStub(ManagedChannelBuilder.forTarget(it)
                    .usePlaintext()
                    .build())
        })
        System.err.println("processed distribution")
        System.err.println("splitting block")

        splitToParts(distribution, shuffleDir, shuffleDir.resolve("block"), serializer, comparator)

        System.err.println("block splitted")
    }

    suspend fun <T> splitToParts(distribution: Adp.Distribution,
                                 shuffleDir: File,
                                 inFile: File,
                                 serializer: SerUtils.Serializer<T>,
                                 comparator: Comparator<T>) {
        withContext(Dispatchers.IO) {
            var blockId = 0
            val partLimits = distribution.partitionsList.map { SerUtils.deserialize(it.toByteArray()) as T }
            System.err.println(partLimits)
            var currentPart = partLimits.first()
            var currentWriter = shuffleDir.resolve("part0").bufferedWriter()

            for (line in inFile.bufferedReader().lines()) {
                val v = serializer.deserialize(line)

                if (blockId < partLimits.size && comparator.compare(v, currentPart) >= 0) {
                    blockId++
                    if (blockId < partLimits.size) {
                        currentPart = partLimits[blockId]
                    }
                    currentWriter.flush()
                    currentWriter.close()
                    blocks.get()[blockId - 1].set(shuffleDir.resolve("part${blockId - 1}"))

                    currentWriter = shuffleDir.resolve("part$blockId").bufferedWriter()
                }

                currentWriter.write(line)
                currentWriter.newLine()
            }
            currentWriter.flush()
            currentWriter.close()
            blocks.get()[blockId].set(shuffleDir.resolve("part$blockId"))
        }
    }

    fun getSample(file: File): List<ByteString> {
        val random = Random(System.currentTimeMillis())
        return file.bufferedReader().lines()
                .filter { random.nextDouble(0.0, 1.0) < SAMPLE_RATE }
                .map { SerUtils.serialize(serializer.deserialize(it)) }
                .map { ByteString.copyFrom(it) }
                .toList()
    }

    override fun readMerged(scope: CoroutineScope): ReceiveChannel<T> {
        System.err.println("reading merged: awaiting")

        return scope.produce {
            val flows = stubs.get().map {
                val partId = partitionId.get()
                val request = Adp.ShuffleInfo.newBuilder()
                        .setShuffleId(shuffleId)
                        .setShuffleWorkerNum(partId)
                        .build()
                it.shuffleRead(request)
            }.toList()

            System.err.println("got flows")

            val channels = (1..flows.size).map { Channel<T>(1000) }

            flows.zip(channels).forEach { (flow, channel) ->
                launch {
                    flow.collect {
                        channel.send(serializer.deserialize(String(it.toByteArray())))
                    }
                    channel.close()
                }
            }

            val pq = PriorityQueue<Pair<T, Int>>(pairComparator<T, Int>(comparator))
            for ((i, channel) in channels.withIndex()) {
                channel.receiveOrNull()?.let {
                    System.err.println("added $it from $i")
                    pq.add(it to i)
                }
            }

            while (!pq.isEmpty()) {
                val (v, i) = pq.poll()
                channels[i].receiveOrNull()?.also { pq.add(it to i) }
                send(v)
            }
        }
    }
}
