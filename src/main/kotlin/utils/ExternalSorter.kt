package utils

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import java.io.BufferedWriter
import java.io.File
import java.util.*
import kotlin.Comparator
import kotlin.NoSuchElementException
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class PeekIterator<T>(val iterator: Iterator<T>): Iterator<T> {
    var t: Optional<T> = Optional.empty()

    override fun hasNext(): Boolean {
        if (t.isPresent) {
            return true
        }
        if (iterator.hasNext()) {
            t = Optional.of(iterator.next())
            return true
        }
        return false
    }

    override fun next(): T {
        hasNext()
        val result = t.orElseGet {
            throw NoSuchElementException()
        }
        t = Optional.empty()
        return result
    }

    fun peek(): T {
        return t.get()
    }
}

class MergedIterator<T>(
        leftIterator: Iterator<T>,
        rightIterator: Iterator<T>,
        private val comparator: Comparator<T>): Iterator<T> {

    private val left = PeekIterator(leftIterator)
    private val right = PeekIterator(rightIterator)

    override fun hasNext(): Boolean {
        return left.hasNext() || right.hasNext()
    }

    override fun next(): T {
        if (!right.hasNext()) {
            return left.next()
        }
        if (!left.hasNext()) {
            return right.next()
        }

        return if (comparator.compare(left.peek(), right.peek()) < 0) {
            left.next()
        } else {
            right.next()
        }
    }
}

class ExternalSorter<T>(private val shuffleDir: File,
                        private val comparator: Comparator<T>,
                        private val tClass: Class<T>,
                        private val bufferSize: Int = 1000) {

    suspend fun sortAndWrite(scope: CoroutineScope, recChannel: ReceiveChannel<T>) {
        val buffer = mutableListOf<T>()
        val serializer = KryoSerializer(tClass)
        var dumpNumber = 0
        recChannel.consumeEach {
            buffer.add(it)
            if (buffer.size >= bufferSize) {
                dumpBuffer(buffer, serializer, dumpNumber++)
            }
        }
        if (buffer.size > 0)
            dumpBuffer(buffer, serializer, dumpNumber++)
        val blocksNumber = dumpNumber
        mergeBlocks(scope, 0, blocksNumber)
        shuffleDir.resolve("shuffle0-$blocksNumber").renameTo(shuffleDir.resolve("block"))
    }

    private suspend fun mergeBlocks(scope: CoroutineScope,
                                    left: Int,
                                    right: Int){
        System.err.println("merging $left - $right")
        if (right - left <= 1) {
            System.err.println("merged $left - $right")
            return
        }
        val middle = (right + left) / 2

        val a1 = scope.async { mergeBlocks(scope, left, middle) }
        a1.await()
        val a2 = scope.async { mergeBlocks(scope, middle, right) }
        a2.await()

        System.err.println("merging files $left $right")
        mergeFiles(left, middle, right)
        System.err.println("merged $left - $right")
    }

    private suspend fun mergeFiles(
            left: Int,
            middle: Int,
            right: Int
    ) {
        withContext(Dispatchers.IO) {
            val serializer = KryoSerializer(tClass)
            val leftFile = shuffleDir.resolve("shuffle$left-$middle")
            val rightFile = shuffleDir.resolve("shuffle$middle-$right")
            val leftIterator = serializer.readFileSync(leftFile)
            val rightIterator = serializer.readFileSync(rightFile)

            val outFile = shuffleDir.resolve("shuffle$left-$right")

            serializer.writeToFile(MergedIterator(leftIterator, rightIterator, comparator), outFile)

            leftFile.delete()
            rightFile.delete()
        }
    }

    private suspend fun dumpBuffer(buffer: MutableList<T>,
                                   serializer: Serializer<T>,
                                   dumpNumber: Int) {
        val outFile = shuffleDir.resolve("shuffle$dumpNumber-${dumpNumber + 1}")
        println("dump $dumpNumber")
        buffer.sortWith(comparator)
        withContext(Dispatchers.IO) {
            serializer.writeToFile(buffer.iterator(), outFile)
        }
        buffer.clear()
    }
}

fun main() {
    val es = ExternalSorter<Int>(File("local"), Comparator { a, b -> b - a }, Int::class.java, 10000)
    measureTimeMillis {
        runBlocking {
            es.sortAndWrite(this, produce {
                val random = Random(System.currentTimeMillis())
                repeat(100) {
                    repeat(1000) {
                        send(Math.abs(random.nextInt()) % 100000)
                    }
                }
            })
        }
    }.also { println("completed in ${it} s") }
}
//100k, 1 buffer -> 22s
//100k, 10000 buffer -> 0.5s
// 100m numbers, 1m buffer -> 114s async
// 100m numbers, 1m buffer -> 145s sync