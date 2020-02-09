import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.reduce
import java.io.File
import java.io.Serializable

abstract class RDDImplAsync<T> {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}

class MappedRDDImplAsync<T, R>(val parent: RDDImplAsync<T>, val f: suspend (T) -> R): RDDImplAsync<R>(), Serializable {
    override fun channel(scope: CoroutineScope): ReceiveChannel<R> {
        return parent.channel(scope).map { f(it) }
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
