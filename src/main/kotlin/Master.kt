import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import api.MAX_CAP
import api.operations.ParallelOperation
import utils.SerUtils
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

interface Master {
    fun <T, R> execute(op: ParallelOperation<T, R>): R
}

class LocalMaster: Master {
    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        return runBlocking {
            op.toImpl().execute(
                this,
                WorkerContext(
                    0,
                    listOf(),
                    ShuffleManager(0, listOf()),
                    CacheManager(100)))
        }
    }
}

class MultiWorkerMaster(private val workers: List<Int>): Master {
    private val client = HttpClient()

    fun test() {
        for (worker in workers) {
            runBlocking(Dispatchers.IO) {
                try {
                    val res = client.get<String>("http://127.0.0.1:$worker/test")
                    println(res)
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
    }

    private fun deserializeResult(byteArray: ByteArray): Any {
        val bis = ObjectInputStream(ByteArrayInputStream(byteArray))
        return bis.readObject()
    }

    private suspend fun runOnWorker(port: Int, serialized: ByteArray, path: String = "/run"): ByteArray {
        println("trying to send code")
        val s = SerUtils.base64encode(serialized)

        val res = client.post<String>("http://127.0.0.1:$port$path") {
            body = s
        }
        return SerUtils.base64decode(res)
    }

    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        val serialized = op.serialize()
        return runBlocking {
            val channel = Channel<R>(MAX_CAP)
            val result = async { op.consumeParts(channel) }
            workers
                .map {
                    async {
                        val ba = runOnWorker(it, serialized, path = "/runAsync")
                        channel.send(deserializeResult(ba) as R)
                    }
                }.awaitAll()
            channel.close()
            result.await()
        }
    }
}