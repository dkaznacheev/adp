package testservice

import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.delay

@Suppress("UNUSED")
class TestService(val port: Int, val delay: Long) {
    private val server = embeddedServer(Netty, port) {
        routing {
            get("/test") {
                call.respondText { "Hello from port $port" }
            }
            get("/echo") {
                delay(delay)
                call.respond(call.parameters["value"] ?: HttpStatusCode.NotAcceptable)
            }
        }
    }

    fun start() {
        server.start()
    }
}
