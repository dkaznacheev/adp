package master

import api.MAX_CAP
import api.operations.ParallelOperation
import api.rdd.RDDImpl
import api.rdd.ReduceByKeyGrpcRDD
import api.rdd.ReduceByKeyGrpcRDDImpl
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import utils.SerUtils
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

class MultiWorkerMaster(private val workers: List<Int>): Master {
    private val client = HttpClient()

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

    override fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>) {

    }

    override fun <K, V> getReduceByKeyRDDImpl(parent: RDDImpl<Pair<K, V>>,
                                              shuffleId: Int,
                                              keyComparator: Comparator<K>,
                                              serializer: SerUtils.Serializer<Pair<K, V>>, f: (V, V) -> V): RDDImpl<Pair<K, V>> {
        return ReduceByKeyGrpcRDDImpl(parent, shuffleId, keyComparator, serializer, f)
    }
}