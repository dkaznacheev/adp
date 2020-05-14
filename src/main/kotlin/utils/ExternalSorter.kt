package utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.withContext
import java.io.BufferedWriter
import java.io.File

class ExternalSorter<T>(private val shuffleDir: File,
                        private val comparator: Comparator<T>,
                        private val serializer: SerUtils.Serializer<T>,
                        private val bufferSize: Int = 1000) {

    suspend fun sortAndWrite(scope: CoroutineScope, recChannel: ReceiveChannel<T>) {
        val buffer = mutableListOf<T>()
        var dumpNumber = 0
        recChannel.consumeEach {
            buffer.add(it)
            if (buffer.size >= bufferSize) {
                dumpBuffer(buffer, dumpNumber++)
            }
        }
        if (buffer.size > 0)
            dumpBuffer(buffer, dumpNumber++)
        val blocksNumber = dumpNumber
        mergeBlocks(scope, 0, blocksNumber)
        shuffleDir.resolve("shuffle0-$blocksNumber").renameTo(shuffleDir.resolve("block"))
    }

    private fun <T> writeObject(bw: BufferedWriter, o: T, serializer: SerUtils.Serializer<T>) {
        bw.write(serializer.serialize(o))
        bw.newLine()
    }

    private suspend fun mergeBlocks(scope: CoroutineScope,
                                    left: Int,
                                    right: Int){
        System.err.println("merging $left - $right")
        if (right - left <= 1) {
            return
        }
        val middle = (right + left) / 2
        val leftMerge = scope.async {
            mergeBlocks(scope, left, middle)
        }
        val rightMerge = scope.async {
            mergeBlocks(scope, middle, right)
        }
        leftMerge.await()
        rightMerge.await()

        mergeFiles(left, middle, right)
        System.err.println("merged $left - $right")
    }

    private suspend fun mergeFiles(
            left: Int,
            middle: Int,
            right: Int
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

    private suspend fun dumpBuffer(buffer: MutableList<T>,
                                   dumpNumber: Int) {
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
}