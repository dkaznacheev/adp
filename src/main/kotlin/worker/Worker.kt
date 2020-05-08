package worker

import Adp
import WorkerGrpcKt
import api.operations.ParallelOperationImpl
import com.google.protobuf.ByteString
import io.grpc.ServerBuilder
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.response.respondFile
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import utils.SerUtils
import java.io.File

class Worker(port: Int) {
    private val shuffleManager = ShuffleManager(port, listOf(8080, 8081))
    private val grpcShuffleManager = GrpcShuffleManager()

    private val cacheManager = CacheManager(100)
    val ctx = WorkerContext(shuffleManager, grpcShuffleManager, cacheManager)

    private suspend fun processRunCall(call: ApplicationCall) {
        try {
            val body = call.receive<String>()
            val serialized = SerUtils.base64decode(body)

            val ropAny = SerUtils.deserialize(serialized)

            val rop = ropAny as ParallelOperationImpl<*, *>
            val result = coroutineScope {
                rop.executeSerializable(this, ctx)
            }

            call.respondText(SerUtils.base64encode(result))
        } catch (e: Exception) {
            e.printStackTrace()
            call.respondText(status = HttpStatusCode.InternalServerError){""}
        }
    }

    private val server = embeddedServer(Netty, port) {
        routing {
            get("/test") {
                call.respondText { "Hello from port $port" }
            }
            post("/runAsync") {
                println("received async call")
                processRunCall(call)
            }
            get("/getBlock") {
                try {
                    shuffleManager.waitForOpen()
                    val blockNum = call.parameters["num"]!!.toInt()
                    val f = shuffleManager.blockOf(blockNum)
                    call.respondFile(f)
                } catch (e: Exception) {
                    println(e.stackTrace)
                    call.respond(HttpStatusCode.InternalServerError)
                }
            }
            get("/testBlock") {
                call.respondFile(File("shuffle/out/worker8080"))
            }
        }
    }

    fun start() {
        server.start(wait = true)
    }

    private inner class ADPServerService: WorkerGrpcKt.WorkerCoroutineImplBase() {
        override suspend fun execute(request: Adp.Operation): Adp.Value {
            val op = SerUtils.deserialize(request.op.toByteArray())
            ctx.workerId = request.workerId
            grpcShuffleManager.workerId = request.workerId
            val rop = op as ParallelOperationImpl<*, *>
            val result = coroutineScope {
                rop.executeSerializable(this, ctx)
            }
            return toGrpcValue(result)
        }

        override fun shuffleRead(info: Adp.ShuffleInfo): Flow<Adp.Value> {
            return grpcShuffleManager.blockFor(info.shuffleId, info.workerId)
        }
    }

    private val rpcServer = ServerBuilder
        .forPort(port - 8080 + 8090)
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