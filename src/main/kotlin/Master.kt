import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

class Master {
    private val workers = listOf(8080, 8081)
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

    fun <T> execute(operation: ReduceOperation<T>): T? {
        val serialized = operation.serialize()
        return workers
            .map {
                runBlocking { runOnWorker(it, serialized) }
            }
            .map {
                println("trying to deser")
                deserializeResult(it) as T
            }.reduce(operation.f)
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

    fun <T, R> executeAsync(op: ParallelOperationAsync<T, R>): R {
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
/*
    fun <T> reduceAsync(op: ReduceOperationAsync<T>): T? {
        val serialized = op.serialize()
        return runBlocking {
            workers
                .map {
                    async {
                        val ba = runOnWorker(it, serialized, path = "/runAsync")
                        deserializeResult(ba) as T
                    }
                }
                .awaitAll()
                .reduce(op.f)
        }
    }

 */
}