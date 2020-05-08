import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import api.MAX_CAP
import api.operations.ParallelOperation
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
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
                    GrpcShuffleManager(),
                    CacheManager(100)))
        }
    }
}

class GrpcMaster(private val port: Int, private val workers: List<String>): Master {
    private val channels = workers.map { address ->
        ManagedChannelBuilder.forTarget(address)
            .usePlaintext()
            .build()
    }

    private val workerStubs = channels.map {
        WorkerGrpcKt.WorkerCoroutineStub(it)
    }

    private val rpcServer = ServerBuilder
            .forPort(port)
            .addService(ADPMasterService())
            .build()

    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        val grpcOp = toGrpcOperation(op)

        rpcServer.start()

        return runBlocking {
            val channel = Channel<R>(MAX_CAP)
            val result = async { op.consumeParts(channel) }

            val distributionChannel = Channel<Adp.WorkerDistribution>()
            waitForDistributions(this, distributionChannel)

            workerStubs.map { worker ->
                async {
                    val response = worker.execute(grpcOp)
                    val ba = response.value.toByteArray()
                    channel.send(SerUtils.deserialize(ba) as R)
                }
            }.awaitAll()

            channel.close()
            result.await()
        }
    }

    private fun waitForDistributions(scope: CoroutineScope, distributionChannel: Channel<Adp.WorkerDistribution>) {
        scope.launch {
            val workersRemaining = workers.toMutableList()
            val distributions = mutableListOf<Pair<Int, Int>>()
            while(workersRemaining.isNotEmpty()) {
                val dst = distributionChannel.receive()
                workersRemaining.remove(dst.workerId)
                //distributions.add(dst.min to dst.max)
            }
        }
    }

    private inner class ADPMasterService: MasterGrpcKt.MasterCoroutineImplBase() {
        override suspend fun getWorkers(request: Adp.Void): Adp.WorkerList {
            return Adp.WorkerList.newBuilder().addAllWorkers(workers).build()
        }

        override suspend fun sampleDistribution(request: Adp.WorkerDistribution): Adp.Distribution {

            return super.sampleDistribution(request)
        }
    }

    companion object {
        fun toGrpcOperation(op: ParallelOperation<*, *>): Adp.Operation {
            return Adp.Operation
                .newBuilder()
                .setOp(ByteString.copyFrom(op.serialize()))
                .build()
        }
    }
}

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
}