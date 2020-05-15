package shuffle

import api.MAX_CAP
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.utils.io.jvm.javaio.copyTo
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import utils.SerUtils
import java.io.File

class LegacyHashShuffleManager(val thisWorker: Int, val workers: List<Int>) {
    private val openBroadcast = Channel<Unit>(workers.size + 1)
    private val outPath = File("shuffle/out")
    private val inPath = File("shuffle/in")

    val outFiles = workers.map {
        outPath.resolve(File("worker$it"))
    }
    val inFiles = workers.map {
        inPath.resolve(File("worker$it"))
    }

    suspend fun startBroadcast() {
        repeat(workers.size) {
            openBroadcast.send(Unit)
        }
    }

    suspend fun waitForOpen() {
        openBroadcast.receive()
    }

    suspend fun <T> loadBlocks(scope: CoroutineScope): ReceiveChannel<T> {
        val httpClient = HttpClient()
        workers.mapIndexed { i, port ->
            scope.async {
                val response = httpClient.get<HttpResponse>("http://localhost:$port/getBlock?num=$thisWorker")
                inFiles[i].outputStream().use {
                    response.content.copyTo(it)
                }
            }
        }.awaitAll()
        return scope.produce(capacity = MAX_CAP) {
            for (file in inFiles) {
                val lines = file.bufferedReader().lineSequence()
                for (line in lines) {
                    send(SerUtils.deserialize(SerUtils.base64decode(line)) as T)
                }
            }
        }
    }

    fun blockOf(i: Int): File {
        return outFiles.zip(workers).find { (_, worker) -> worker == i}!!.first
    }

    suspend fun <T, R> createBlocks(channel: ReceiveChannel<Pair<T, R>>) {
        withContext(Dispatchers.IO) {
            println("saving parts")
            val outStreams = outFiles.map { it.outputStream() }
            val writers = outStreams.map { it.bufferedWriter() }

            for ((k, v) in channel) {
                val i = Math.floorMod(k.hashCode(), workers.size)
                val s = SerUtils.base64encode(SerUtils.serialize(k to v))
                println("saving $v to $i part: $s")
                writers[i].write(s)
                writers[i].write("\n")
            }
            println()
            for ((outStream, writer) in outStreams.zip(writers)) {
                writer.flush()
                outStream.close()
            }
        }
    }
}