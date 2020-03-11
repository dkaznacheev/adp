import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.request.receiveChannel
import io.ktor.response.respondBytes
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.cio.toByteArray
import kotlinx.coroutines.coroutineScope
import kotlin.coroutines.coroutineContext

class Worker(port: Int) {
    private suspend fun processRunCall(call: ApplicationCall, isAsync: Boolean = false) {
        try {
            val body = call.receive<String>()
            val serialized = SerUtils.base64decode(body)

            val ropAny = SerUtils.deserialize(serialized)
            val result = if (!isAsync) {
                val rop = ropAny as ReduceOperationImpl<*>
                rop.executeSerializable()
            } else {
                val rop = ropAny as ParallelOperationImplAsync<*, *>
                coroutineScope {
                    rop.executeSerializable(this)
                }
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
            post("/run") {
                println("received a call")
                processRunCall(call)
            }
            post("/runAsync") {
                println("received async call")
                processRunCall(call, isAsync = true)
            }
        }
    }

    fun start() {
        server.start(wait = true)
    }
}