package master

import Adp
import MasterGrpcKt
import WorkerGrpcKt
import api.MAX_CAP
import api.operations.ParallelOperation
import api.rdd.RDDImpl
import api.rdd.ReduceByKeyGrpcRDDImpl
import api.rdd.SortedGrpcRDDImpl
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import utils.KryoSerializer
import utils.NPair

class GrpcMaster(port: Int, private val workers: List<String>): Master {
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

    override fun <K, V> getReduceByKeyRDDImpl(parent: RDDImpl<NPair<K, V>>,
                                              shuffleId: Int,
                                              keyComparator: (K, K) -> Int,
                                              tClass: Class<NPair<K, V>>,
                                              f: (V, V) -> V): RDDImpl<NPair<K, V>> {
        return ReduceByKeyGrpcRDDImpl(parent, shuffleId, keyComparator, tClass, f)
    }

    override fun <T> getSortedRDDImpl(parent: RDDImpl<T>, shuffleId: Int, comparator: (T, T) -> Int, tClass: Class<T>): RDDImpl<T> {
        return SortedGrpcRDDImpl(parent, shuffleId, comparator, tClass)
    }

    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        rpcServer.start()

        val opSerialized = op.serialize()

        return runBlocking {
            val channel = Channel<R>(MAX_CAP)
            val result = async { op.consumeParts(channel) }

            shuffleManagers.forEach { (_, manager) -> manager.listenForDistributions(this, workers) }

            val resultSerializer = KryoSerializer(op.rClass)

            workerStubs.zip(workers).map { (worker, id) ->
                async {
                    val grpcOp = toGrpcOperation(opSerialized, id)
                    try {
                        val response = worker.execute(grpcOp)
                        val ba = response.value.toByteArray()
                        channel.send(resultSerializer.deserialize(ba))
                    } catch (e: Throwable) {
                        e.printStackTrace()
                        error(e)
                    }
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
        fun toGrpcOperation(op: ByteArray, workerId: String): Adp.Operation {
            return Adp.Operation.newBuilder()
                .setOp(ByteString.copyFrom(op))
                .setWorkerId(workerId)
                .build()
        }
    }
}