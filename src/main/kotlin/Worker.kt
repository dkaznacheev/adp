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

class Worker(port: Int) {
    private val server = embeddedServer(Netty, port) {
        routing {
            get("/test") {
                call.respondText { "Hello from port $port" }
            }
            post("/run") {
                try {
                    println("received a call")
                    val body = call.receive<String>()
                    val serialized = SerUtils.base64decode(body)
                    val rop = SerUtils.deserialize(serialized) as ReduceOperationImpl<*>
                    val result = rop.executeSerializable()
                    call.respondText(SerUtils.base64encode(result))
                } catch (e: Exception) {
                    e.printStackTrace()
                    call.respondText(status = HttpStatusCode.InternalServerError){""}
                }
            }
        }
    }

    fun start() {
        server.start(wait = true)
    }
}