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
import java.lang.Exception
import java.util.Comparator

class GrpcShuffleManager {
    private val outPath = File("shuffle/outg")
    private val BUFFER_SIZE = 1000

    fun blockFor(shuffleId: Int, worker: Int): Flow<Adp.Value> {
        return flow {

        }
    }


    private suspend fun <T> sortAndWrite(scope: CoroutineScope,
                                         recChannel: ReceiveChannel<T>,
                                         shuffleDir: File,
                                         comparator: Comparator<T>) {
        val buffer = mutableListOf<T>()
        var dumpNumber = 0
        recChannel.consumeEach {
            buffer.add(it)
            if (buffer.size >= BUFFER_SIZE) {
                dumpBuffer(buffer, dumpNumber++, shuffleDir, comparator)
            }
        }
        if (buffer.size > 0)
            dumpBuffer(buffer, dumpNumber++, shuffleDir, comparator)
        val blocksNumber = dumpNumber
        mergeBlocks(scope, shuffleDir, comparator, 0, blocksNumber)
    }

    private fun writeObject(bw: BufferedWriter, o: Any?) {
        bw.write(SerUtils.wrap(o))
        bw.newLine()
    }

    private suspend fun <T> mergeBlocks(scope: CoroutineScope, shuffleDir: File, comparator: Comparator<T>, left: Int, right: Int) {
        System.err.println("merging $left - $right")
        if (right - left <= 1) {
            return
        }
        val middle = (right + left) / 2
        val leftMerge = scope.async {
            mergeBlocks(scope, shuffleDir, comparator, left, middle)
        }
        val rightMerge = scope.async {
            mergeBlocks(scope, shuffleDir, comparator, middle, right)
        }
        leftMerge.await()
        rightMerge.await()

        mergeFiles(shuffleDir, left, middle, right, comparator)
        System.err.println("merged $left - $right")
    }

    private suspend fun <T> mergeFiles(
        shuffleDir: File,
        left: Int,
        middle: Int,
        right: Int,
        comparator: Comparator<T>
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
                    leftItem = SerUtils.unwrap(leftLines.next()) as T
                }
                if (rightLines.hasNext() && rightItem == null) {
                    rightItem = SerUtils.unwrap(rightLines.next()) as T
                }

                if (rightItem == null) {
                    writeObject(bw, leftItem)
                    leftItem = null
                } else {
                    if (leftItem == null || comparator.compare(leftItem, rightItem) >= 0) {
                        writeObject(bw, rightItem)
                        rightItem = null
                    } else {
                        writeObject(bw, leftItem)
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
                               comparator: Comparator<T>) {
        val outFile = shuffleDir.resolve("shuffle$dumpNumber-${dumpNumber + 1}")

        buffer.sortWith(comparator)
        withContext(Dispatchers.IO) {
            val bw = outFile.outputStream().bufferedWriter()
            for (element in buffer) {
                writeObject(bw, element)
            }
            bw.flush()
            bw.close()
        }
        buffer.clear()
    }

    suspend fun <T> writeAndBroadcast(
            scope: CoroutineScope,
            recChannel: ReceiveChannel<T>,
            shuffleId: Int,
            comparator: Comparator<T>) {
        val shuffleDir = outPath.resolve("shuffle$shuffleId")
        if (!shuffleDir.exists()) {
            shuffleDir.mkdir()
        }
        sortAndWrite(scope, recChannel, shuffleDir, comparator)

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
        File("shuffle/outg/shuffle123/shuffle0-10").inputStream().bufferedReader().lines().forEach {
            SerUtils.deserialize(SerUtils.base64decode(it)).also { println(it) }
        }
    }
}