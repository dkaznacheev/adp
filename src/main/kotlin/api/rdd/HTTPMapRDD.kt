package api.rdd

import api.CONCURRENT_MAP_LIMIT
import worker.WorkerContext
import api.MAX_CAP
import io.ktor.client.HttpClient
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class HTTPMapRDD<T, R>(val parent: RDD<T>, rClass: Class<R>, val f: suspend HttpClient.(T) -> R): RDD<R>(parent.master, rClass) {
    override fun toImpl(): RDDImpl<R> {
        return HTTPMapRDDImpl(parent.toImpl(), f)
    }
}

class HTTPMapRDDImpl<T, R>(val parent: RDDImpl<T>, val f: suspend HttpClient.(T) -> R): RDDImpl<R>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope, ctx: WorkerContext): ReceiveChannel<R> {
        val client = HttpClient()
        val channel = Channel<R>(MAX_CAP)
        val recChannel = parent.channel(scope, ctx)
        scope.launch {
            (1..CONCURRENT_MAP_LIMIT).map {
                async {
                    for (t in recChannel) {
                        channel.send(client.f(t))
                    }
                }
            }.awaitAll()
            channel.close()
            client.close()
        }
        return channel
    }
}
