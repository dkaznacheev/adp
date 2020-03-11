import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.cio.writeChannel
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import java.io.File
import java.io.Serializable

const val MAX_CAP = 1000

abstract class RDDImplAsync<T> : Serializable {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}

class MappedRDDImplAsync<T, R>(val parent: RDDImplAsync<T>, val f: suspend (T) -> R): RDDImplAsync<R>() {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope): ReceiveChannel<R> {
        val channel = Channel<R>(MAX_CAP)
        val recChannel = parent.channel(scope)
        scope.launch {
            val defs = mutableListOf<Deferred<*>>()
            while(!recChannel.isClosedForReceive) {
                val t = recChannel.receiveOrNull() ?: break
                defs.add(async {
                    channel.send(f(t))
                })
            }
            defs.awaitAll()
            channel.close()
        }
        return channel
    }
}

@ExperimentalCoroutinesApi
class SourceRDDImplAsync(val filename: String): RDDImplAsync<String>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<String> {
        val lines = File(filename).bufferedReader().lineSequence()
        return scope.produce {
            for (line in lines) {
                send(line)
            }
        }
    }
}

abstract class ParallelOperationImplAsync<T, R>(val rdd: RDDImplAsync<T>): Serializable {
    abstract suspend fun execute(scope: CoroutineScope) : R

    open suspend fun executeSerializable(scope: CoroutineScope): ByteArray {
        val result = execute(scope) as Serializable
        return SerUtils.serialize(result)
    }
}

class ReduceOperationImplAsync<T>(rdd: RDDImplAsync<T>, val f: (T, T) -> T): ParallelOperationImplAsync<T, T>(rdd) {
    override suspend fun execute(scope: CoroutineScope): T {
        return rdd.channel(scope).reduce(f)
    }
}

class SaveAsObjectOperationImplAsync<T>(rdd: RDDImplAsync<T>, val name: String): ParallelOperationImplAsync<T, Unit>(rdd) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope) {
        val recChannel = rdd.channel(scope)
        val outChannel = File(name).writeChannel()
        scope.launch(Dispatchers.IO) {
            recChannel.consumeEach {
                val ba = SerUtils.serialize(it)
                outChannel.writeFully(ba, 0, ba.size)
            }
        }
    }
}

class ReduceByKeyOperationImplAsync<K, T>(val parent: RDDImplAsync<Pair<K, T>>, val f: (T, T) -> T): RDDImplAsync<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<Pair<K, T>> {
        val channel = Channel<Pair<K, T>>(MAX_CAP)
        val recChannel = parent.channel(scope)

        scope.launch {
            // TODO
        }
        return channel
    }
}