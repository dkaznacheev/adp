package shuffle

import Adp
import MasterGrpcKt
import api.rdd.pairComparator
import com.esotericsoftware.kryo.io.Output
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import utils.ExternalSorter
import utils.LazyChannel
import utils.SerUtils
import worker.WorkerContext
import java.io.File
import java.io.FileOutputStream
import java.util.*
import kotlin.random.Random

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
    private val blocks = LazyChannel<List<File>>()
    private val stubs = LazyChannel<List<WorkerGrpcKt.WorkerCoroutineStub>>()

    fun blockFor(workerNum: Int): Flow<Adp.Value> {
        System.err.println("got request for $workerNum")
        return flow {
            System.err.println("awaiting part$workerNum")
            val file = blocks.get()[workerNum]
            serializer.readFileFlow(file, this)
        }.map {
            Adp.Value.newBuilder()
                    .setValue(ByteString.copyFrom(serializer.serialize(it)))
                    .build()
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

        //blocks.set((0 until distribution.partitionsList.size).map { LazyChannel<File>() })
        stubs.set(distribution.workersList.map {
            WorkerGrpcKt.WorkerCoroutineStub(ManagedChannelBuilder.forTarget(it)
                    .usePlaintext()
                    .build())
        })
        System.err.println("processed distribution")
        System.err.println("splitting block")


        splitToParts(distribution, shuffleDir, shuffleDir.resolve("block"), serializer, comparator)
        val b = (0..distribution.partitionsList.size).map { shuffleDir.resolve("part$it") }
        blocks.set(b)

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
            var currentOutput = Output(FileOutputStream(shuffleDir.resolve("part0")))

            for (v in serializer.readFileSync(inFile)) {
                if (blockId < partLimits.size && comparator.compare(v, currentPart) >= 0) {
                    blockId++
                    if (blockId < partLimits.size) {
                        currentPart = partLimits[blockId]
                    }
                    currentOutput.flush()
                    currentOutput.close()
                    System.err.println("setting part${blockId - 1}")

                    System.err.println("set part${blockId - 1}")
                    currentOutput = Output(FileOutputStream(shuffleDir.resolve("part$blockId")))
                }

                serializer.writeToOutput(currentOutput, v)
            }
            currentOutput.flush()
            currentOutput.close()
            System.err.println("setting part$blockId")
            //blocks.get()[blockId].set(shuffleDir.resolve("part$blockId"))
            System.err.println("set part$blockId")
        }
    }

    fun getSample(file: File): List<ByteString> {
        val random = Random(System.currentTimeMillis())
        return serializer.readFileSync(file).asSequence()
                .filter { random.nextDouble(0.0, 1.0) < SAMPLE_RATE }
                .map { ByteString.copyFrom(serializer.serialize(it)) }
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

            val channels = flows.map { flow ->
                produce {
                    flow.collect { send(serializer.deserialize(it.value.toByteArray())) }
                }
            }

            val pq = PriorityQueue<Pair<T, Int>>(pairComparator<T, Int>(comparator))
            for ((i, channel) in channels.withIndex()) {
                channel.receiveOrNull()?.let {
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
