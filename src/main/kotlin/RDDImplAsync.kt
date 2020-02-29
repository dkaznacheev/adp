import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import java.io.File
import java.io.Serializable

const val MAX_CAP = 1000

abstract class RDDImplAsync<T> {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}

class MappedRDDImplAsync<T, R>(val parent: RDDImplAsync<T>, val f: suspend (T) -> R): RDDImplAsync<R>(), Serializable {
    @ExperimentalCoroutinesApi
    override fun channel(scope: CoroutineScope): ReceiveChannel<R> {
        //val channel = parent.channel(scope).map { scope.la }
        //return parent.channel(scope).map { f(it) }
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
        /*
        return scope.produce {
            parent.channel(scope).consumeEach {
                println("received $it")
                val res = f(it)
                println("processed $it")
                send(res)
            }
        }*/
    }
}

@ExperimentalCoroutinesApi
class SourceRDDImplAsync(val filename: String): RDDImplAsync<String>(), Serializable {
    override fun channel(scope: CoroutineScope): ReceiveChannel<String> {
        val lines = File(filename).bufferedReader().lineSequence()
        return scope.produce {
            for (line in lines) {
                send(line)
            }
        }
    }
}

class ReduceOperationImplAsync<T>(val rdd: RDDImplAsync<T>, val f: (T, T) -> T): Serializable {
    suspend fun execute(scope: CoroutineScope): T {
        return rdd.channel(scope).reduce(f)
    }

    suspend fun executeSerializable(scope: CoroutineScope): ByteArray {
        val result = execute(scope) as Serializable
        return SerUtils.serialize(result)
    }
}
