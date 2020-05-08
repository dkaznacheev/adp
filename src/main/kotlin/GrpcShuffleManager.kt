import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import utils.SerUtils
import java.io.BufferedWriter
import java.io.File
import java.util.Comparator

class GrpcShuffleManager {
    //private val myId = File("id").readText()
    private val masterAddress = "localhost:8090"
    @PublishedApi internal val outPath = File("shuffle/outg")
    private val BUFFER_SIZE = 1000

    private val masterStub = MasterGrpcKt.MasterCoroutineStub(ManagedChannelBuilder.forTarget(masterAddress)
            .usePlaintext()
            .build())

    fun blockFor(shuffleId: Int, worker: String): Flow<Adp.Value> {
        return flow {

        }
    }

    @PublishedApi internal suspend fun <T> sortAndWrite(
        scope: CoroutineScope,
        recChannel: ReceiveChannel<T>,
        shuffleDir: File,
        comparator: Comparator<T>,
        serializer: SerUtils.Serializer<T>
    ) {
        val buffer = mutableListOf<T>()
        var dumpNumber = 0
        recChannel.consumeEach {
            buffer.add(it)
            if (buffer.size >= BUFFER_SIZE) {
                dumpBuffer(buffer, dumpNumber++, shuffleDir, comparator, serializer)
            }
        }
        if (buffer.size > 0)
            dumpBuffer(buffer, dumpNumber++, shuffleDir, comparator, serializer)
        val blocksNumber = dumpNumber
        mergeBlocks(scope, shuffleDir, comparator, serializer, 0, blocksNumber)
        shuffleDir.resolve("shuffle0-$blocksNumber").renameTo(shuffleDir.resolve("block"))
//
//        val (min, max) = findMinMax<T>(shuffleDir.resolve("block"))
//        val request = Adp.WorkerDistribution.newBuilder()
//                .setMin(ByteString.copyFrom(SerUtils.serialize(min)))
//                .setMax(ByteString.copyFrom(SerUtils.serialize(max)))
//                .setWorkerId(myId)
//                .build()
//
//        val distribution = masterStub.sampleDistribution(request)

    }

    private suspend fun <T> findMinMax(file: File): Pair<T, T> {
        return withContext(Dispatchers.IO) {
            var first = file.bufferedReader().lineSequence().first()
            var last: String? = null
            file.bufferedReader().lineSequence().forEach {
                last = it
            }
            SerUtils.unwrap(first) as T to SerUtils.unwrap(last!!) as T
        }
    }

    private fun <T> writeObject(bw: BufferedWriter, o: T, serializer: SerUtils.Serializer<T>) {
        bw.write(serializer.serialize(o))
        bw.newLine()
    }

    private suspend fun <T> mergeBlocks(scope: CoroutineScope,
                                        shuffleDir: File,
                                        comparator: Comparator<T>,
                                        serializer: SerUtils.Serializer<T>,
                                        left: Int,
                                        right: Int){
        System.err.println("merging $left - $right")
        if (right - left <= 1) {
            return
        }
        val middle = (right + left) / 2
        val leftMerge = scope.async {
            mergeBlocks(scope, shuffleDir, comparator, serializer, left, middle)
        }
        val rightMerge = scope.async {
            mergeBlocks(scope, shuffleDir, comparator, serializer, middle, right)
        }
        leftMerge.await()
        rightMerge.await()

        mergeFiles(shuffleDir, left, middle, right, comparator, serializer)
        System.err.println("merged $left - $right")
    }

    private suspend fun <T> mergeFiles(
        shuffleDir: File,
        left: Int,
        middle: Int,
        right: Int,
        comparator: Comparator<T>,
        serializer: SerUtils.Serializer<T>
    ) {
        withContext(Dispatchers.IO) {
            val leftFile = shuffleDir.resolve("shuffle$left-$middle")
            val rightFile = shuffleDir.resolve("shuffle$middle-$right")
            val leftLines = leftFile.inputStream().bufferedReader().lineSequence().iterator()
            val rightLines = rightFile.inputStream().bufferedReader().lineSequence().iterator()

            val outFile = shuffleDir.resolve("shuffle$left-$right")
            val bw = outFile.bufferedWriter()

            var leftItem: T? = null
            var rightItem: T? = null
            while (leftLines.hasNext() || rightLines.hasNext()) {
                if (leftLines.hasNext() && leftItem == null) {
                    leftItem = serializer.deserialize(leftLines.next())
                }
                if (rightLines.hasNext() && rightItem == null) {
                    rightItem = serializer.deserialize(rightLines.next())
                }

                if (rightItem == null) {
                    writeObject(bw, leftItem!!, serializer)
                    leftItem = null
                } else {
                    if (leftItem == null || comparator.compare(leftItem, rightItem) >= 0) {
                        writeObject(bw, rightItem, serializer)
                        rightItem = null
                    } else {
                        writeObject(bw, leftItem, serializer)
                        leftItem = null
                    }
                }
            }
            bw.flush()
            bw.close()

            leftFile.delete()
            rightFile.delete()
        }
    }

    private suspend fun <T> dumpBuffer(buffer: MutableList<T>,
                               dumpNumber: Int,
                               shuffleDir: File,
                               comparator: Comparator<T>,
                               serializer: SerUtils.Serializer<T>) {
        val outFile = shuffleDir.resolve("shuffle$dumpNumber-${dumpNumber + 1}")

        buffer.sortWith(comparator)
        withContext(Dispatchers.IO) {
            val bw = outFile.outputStream().bufferedWriter()
            for (element in buffer) {
                writeObject(bw, element, serializer)
            }
            bw.flush()
            bw.close()
        }
        buffer.clear()
    }

    suspend inline fun <reified T> writeAndBroadcast(
            scope: CoroutineScope,
            recChannel: ReceiveChannel<T>,
            shuffleId: Int,
            comparator: Comparator<T>) {
        val shuffleDir = outPath.resolve("shuffle$shuffleId")
        if (!shuffleDir.exists()) {
            shuffleDir.mkdir()
        }
        val serializer = SerUtils.getSerializer<T>()
        sortAndWrite(scope, recChannel, shuffleDir, comparator, serializer)
        //val workers =
    }

    fun <T> readMerged(scope: CoroutineScope, shuffleId: Int): ReceiveChannel<T> {
        return Channel(10)
    }
}

fun main() {
    val sm = GrpcShuffleManager()
    runBlocking {
        val channel = produce {
            for (i in (1..10000)) {
                send(i)
            }
        }
        sm.writeAndBroadcast(this, channel, 123, kotlin.Comparator{a, b -> a - b})
        val serializer = SerUtils.getSerializer<Int>()
        File("shuffle/outg/shuffle123/block").inputStream().bufferedReader().lines().forEach {
            println(serializer.deserialize(it))
        }
    }
}