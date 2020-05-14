package shuffle

import Adp
import MasterGrpcKt
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import utils.ExternalSorter
import utils.SerUtils
import worker.WorkerContext
import java.io.File
import java.util.Comparator
import kotlin.random.Random
import kotlin.streams.toList
import kotlin.system.measureTimeMillis

class GrpcShuffleManager<T>(val ctx: WorkerContext,
                            private val shuffleId: Int,
                            private val comparator: Comparator<T>,
                            private val serializer: SerUtils.Serializer<T>): WorkerShuffleManager<T> {
    private val masterAddress = "localhost:8090"
    private val outPath = File("shuffle/outg")
    private val shuffleDir = outPath.resolve("shuffle$shuffleId")

    private val SAMPLE_RATE = 1.0

    private val masterStub = MasterGrpcKt.MasterCoroutineStub(ManagedChannelBuilder.forTarget(masterAddress)
            .usePlaintext()
            .build())

    fun blockFor(shuffleId: Int, worker: String): Flow<Adp.Value> {
        return flow {

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

        val tstparts = listOf(333333, 666666).map { ByteString.copyFrom(SerUtils.serialize(it)) }
        val distribution = masterStub.sampleDistribution(request)
        splitToParts(distribution, shuffleDir, shuffleDir.resolve("block"), serializer, comparator)
    }

    suspend fun <T> splitToParts(distribution: Adp.Distribution,
                                 shuffleDir: File,
                                 inFile: File,
                                 serializer: SerUtils.Serializer<T>,
                                 comparator: Comparator<T>) {
        withContext(Dispatchers.IO) {
            var blockId = 0
            val partLimits = distribution.partitionsList.map { SerUtils.deserialize(it.toByteArray()) as T }
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
                    currentWriter = shuffleDir.resolve("part$blockId").bufferedWriter()
                }

                currentWriter.write(line)
                currentWriter.newLine()
            }
            currentWriter.flush()
            currentWriter.close()
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

    override fun readMerged(scope: CoroutineScope, shuffleId: Int): ReceiveChannel<T> {

        return Channel(10)
    }
}

fun main() {
    val sm = GrpcShuffleManager(WorkerContext.stub(), 1, kotlin.Comparator { a, b -> a - b }, SerUtils.getSerializer<Int>())
    runBlocking {
        val channel = produce {
            for (i in (1..1000000)) {
                send(i)
            }
        }
        measureTimeMillis {
            sm.writeAndBroadcast(this, channel)
        }.also { println(it) }
//        val serializer = SerUtils.getSerializer<Int>()
//        File("shuffle/outg/shuffle123/block").inputStream().bufferedReader().lines().forEach {
//            println(serializer.deserialize(it))
//        }
    }
}