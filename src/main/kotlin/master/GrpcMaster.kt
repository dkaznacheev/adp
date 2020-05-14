package master

import Adp
import MasterGrpcKt
import WorkerGrpcKt
import api.MAX_CAP
import api.operations.ParallelOperation
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import utils.SerUtils

class GrpcMaster(private val port: Int, private val workers: List<String>): Master {
    private val shuffleManagers = mutableMapOf<Int, MasterShuffleManager<*>>()

    private val channels = workers.map { address ->
        ManagedChannelBuilder.forTarget(address)
            .usePlaintext()
            .build()
    }

    private val workerStubs = channels.map {
        WorkerGrpcKt.WorkerCoroutineStub(it)
    }

    private val rpcServer = ServerBuilder.forPort(port)
            .addService(ADPMasterService())
            .build()

    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        rpcServer.start()

        return runBlocking {
            val channel = Channel<R>(MAX_CAP)
            val result = async { op.consumeParts(channel) }

            shuffleManagers.forEach { (_, manager) -> manager.listenForDistributions(this, workers) }

            workerStubs.zip(workers).map { (worker, id) ->
                async {
                    val grpcOp = toGrpcOperation(op, id)
                    val response = worker.execute(grpcOp)
                    val ba = response.value.toByteArray()
                    channel.send(SerUtils.deserialize(ba) as R)
                }
            }.awaitAll()

            channel.close()
            result.await()
        }
    }

    override fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>) {
        shuffleManagers[masterShuffleManager.shuffleId] = masterShuffleManager
    }

    private inner class ADPMasterService: MasterGrpcKt.MasterCoroutineImplBase() {
        override suspend fun getWorkers(request: Adp.Void): Adp.WorkerList {
            return Adp.WorkerList.newBuilder().addAllWorkers(workers).build()
        }

        override suspend fun sampleDistribution(request: Adp.WorkerDistribution): Adp.Distribution {
            return shuffleManagers[request.shuffleId]?.sampleDistribution(request, workers) ?:
                super.sampleDistribution(request)
        }
    }

    companion object {
        fun toGrpcOperation(op: ParallelOperation<*, *>, workerId: String): Adp.Operation {
            return Adp.Operation.newBuilder()
                .setOp(ByteString.copyFrom(op.serialize()))
                .setWorkerId(workerId)
                .build()
        }
    }
}