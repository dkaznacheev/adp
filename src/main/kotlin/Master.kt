import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

class Master {
    private val workers = listOf(8080)
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
                runOnWorker(it, serialized)
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

    private fun runOnWorker(port: Int, serialized: ByteArray): ByteArray {
        println("trying to send code")
        val s = SerUtils.base64encode(serialized)
        return runBlocking(Dispatchers.IO){
            val res = client.post<String>("http://127.0.0.1:$port/run") {
                body = s
            }
            SerUtils.base64decode(res)
        }
    }
}