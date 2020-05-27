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
import kotlinx.coroutines.flow.*
import utils.*
import worker.WorkerContext
import java.io.File
import java.io.FileOutputStream
import java.util.*
import kotlin.random.Random

class GrpcShuffleManager<T>(val ctx: WorkerContext,
                            private val shuffleId: Int,
                            private val comparator: Comparator<T>,
                            private val tClass: Class<T>): WorkerShuffleManager<T> {
    private val masterAddress = ctx.masterAddress

    private val outPath = File("shuffle/outg")
    private val shuffleDir = outPath.resolve("shuffle$shuffleId")

    private val SAMPLE_RATE = ctx.sampleRate
    private val blockSize = ctx.blockSize
    private val blockBufferSize = ctx.blockBufferSize

    private val masterStub = MasterGrpcKt.MasterCoroutineStub(ManagedChannelBuilder.forTarget(masterAddress)
            .usePlaintext()
            .build())

    private val partitionId = LazyChannel<Int>()
    private val blocks = LazyChannel<List<File>>()
    private val stubs = LazyChannel<List<WorkerGrpcKt.WorkerCoroutineStub>>()

    fun blockFor(workerNum: Int): Flow<Adp.ValueBlock> {
        System.err.println("got request for $workerNum")
        val serializer = KryoSerializer(tClass)
        return flow<List<T>> {
            System.err.println("awaiting part$workerNum")
            val file = blocks.get()[workerNum]
            try {
                serializer.readFileFlow(file, blockBufferSize, this)
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }.flowOn(Dispatchers.IO).map<List<T>, Adp.ValueBlock> { values ->
            println("sending block")
            val serialized = values.map {
               ByteString.copyFrom(serializer.serialize(it))
            }
            Adp.ValueBlock.newBuilder().addAllValues(serialized).build()
        }.flowOn(Dispatchers.IO)
    }

    override suspend fun writeAndBroadcast(
            scope: CoroutineScope,
            recChannel: ReceiveChannel<T>) {
        if (!shuffleDir.exists()) {
            shuffleDir.mkdir()
        }

        ExternalSorter(shuffleDir, comparator, tClass, blockSize).sortAndWrite(scope, recChannel)
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


        splitToParts(distribution, shuffleDir, shuffleDir.resolve("block"), comparator)
        val b = (0..distribution.partitionsList.size).map { shuffleDir.resolve("part$it") }
        blocks.set(b)

        System.err.println("block splitted")
    }

    suspend fun splitToParts(distribution: Adp.Distribution,
                             shuffleDir: File,
                             inFile: File,
                             comparator: Comparator<T>) {
        withContext(Dispatchers.IO) {
            var blockId = 0
            val serializer = KryoSerializer(tClass)
            val partLimits = distribution.partitionsList.map { serializer.deserialize(it.toByteArray()) }
            System.err.println(partLimits)
            if (partLimits.isEmpty()) {
                inFile.renameTo(shuffleDir.resolve("part0"))
                return@withContext
            }

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
        val serializer = KryoSerializer(tClass)
        return serializer.readFileSync(file).asSequence()
                .filter { random.nextDouble(0.0, 1.0) < SAMPLE_RATE }
                .map { ByteString.copyFrom(serializer.serialize(it)) }
                .toList()
    }

    override fun readMerged(scope: CoroutineScope): ReceiveChannel<T> {
        System.err.println("reading merged: awaiting")

        return scope.produce(capacity = blockSize) {
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
                produce(capacity = blockSize / flows.size) {
                    val serializer = KryoSerializer(tClass)
                    flow.buffer(blockSize / flows.size).collect {
                        for (value in it.valuesList) {
                            send(serializer.deserialize(value.toByteArray()))
                        }
                    }
                }
            }

            val pq = PriorityQueue<NPair<T, Int>>(pairComparator<T, Int>(comparator))
            for ((i, channel) in channels.withIndex()) {
                channel.receiveOrNull()?.let {
                    pq.add(it toN i)
                }
            }

            while (!pq.isEmpty()) {
                val (v, i) = pq.poll()
                channels[i].receiveOrNull()?.also { pq.add(it toN i) }
                println("got $v")
                send(v)
            }
        }
    }
}