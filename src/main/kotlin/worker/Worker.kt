package worker

import Adp
import WorkerGrpcKt
import api.operations.ParallelOperationImpl
import com.google.protobuf.ByteString
import io.grpc.ServerBuilder
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import shuffle.GrpcShuffleManager
import utils.SerUtils

class Worker(port: Int) {
    private val shuffleManagers = mutableMapOf<Int, GrpcShuffleManager<*>>()

    private val cacheManager = CacheManager(100)
    val ctx = WorkerContext(shuffleManagers, cacheManager)

    private inner class ADPServerService: WorkerGrpcKt.WorkerCoroutineImplBase() {
        override suspend fun execute(request: Adp.Operation): Adp.Value {
            println("got request")
            val op = SerUtils.deserialize(request.op.toByteArray())
            ctx.workerId = request.workerId
            val rop = op as ParallelOperationImpl<*, *>
            val result = coroutineScope {
                try {
                    rop.executeSerializable(this, ctx)
                } catch (e: Throwable) {
                    e.printStackTrace()
                    error(e)
                }
            }
            println("finished op with $result")
            return toGrpcValue(result)
        }

        @ExperimentalCoroutinesApi
        override fun shuffleRead(request: Adp.ShuffleInfo): Flow<Adp.Value> {
            return shuffleManagers[request.shuffleId]!!.blockFor(request.shuffleWorkerNum)
        }
    }

    private val rpcServer = ServerBuilder
        .forPort(port)
        .addService(ADPServerService())
        .build()

    fun startRPC() {
        rpcServer.start()
        println("Server started, listening on ${rpcServer.port}")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                rpcServer.shutdown()
                println("*** server shut down")
            }
        )
        rpcServer.awaitTermination()
    }

    companion object {
        private fun toGrpcValue(ba: ByteArray): Adp.Value {
            return Adp.Value.newBuilder().setValue(ByteString.copyFrom(ba)).build()
        }
    }
}