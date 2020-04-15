package rdd

import utils.SerUtils
import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.cio.writeChannel
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import java.io.File
import java.io.Serializable

const val MAX_CAP = 1000
const val SUCCESS: Byte = 1

abstract class RDDImpl<T> : Serializable {
    abstract fun channel(scope: CoroutineScope): ReceiveChannel<T>
}

class MappedRDDImpl<T, R>(val parent: RDDImpl<T>, val f: suspend (T) -> R): RDDImpl<R>() {
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
class SourceRDDImpl(val filename: String): RDDImpl<String>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<String> {
        val lines = File(filename).bufferedReader().lineSequence()
        return scope.produce {
            for (line in lines) {
                send(line)
            }
        }
    }
}

abstract class ParallelOperationImpl<T, R>(val rdd: RDDImpl<T>): Serializable {
    abstract suspend fun execute(scope: CoroutineScope) : R

    open suspend fun executeSerializable(scope: CoroutineScope): ByteArray {
        val result = execute(scope) as Serializable
        return SerUtils.serialize(result)
    }
}

class ReduceOperationImpl<T>(rdd: RDDImpl<T>, val f: (T, T) -> T): ParallelOperationImpl<T, T>(rdd) {
    override suspend fun execute(scope: CoroutineScope): T {
        return rdd.channel(scope).reduce(f)
    }
}

class SaveAsObjectOperationImpl<T>(rdd: RDDImpl<T>, val name: String): ParallelOperationImpl<T, Byte>(rdd) {
    @KtorExperimentalAPI
    override suspend fun execute(scope: CoroutineScope): Byte {
        val recChannel = rdd.channel(scope)
        val outChannel = File(name).writeChannel()
        return withContext(Dispatchers.IO) {
            recChannel.consumeEach {
                val ba = SerUtils.serialize(it)
                outChannel.writeFully(ba, 0, ba.size)
            }
            SUCCESS
        }
    }
}

class ReduceByKeyOperationImpl<K, T>(val parent: RDDImpl<Pair<K, T>>, val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<Pair<K, T>> {
        val channel = Channel<Pair<K, T>>(MAX_CAP)
        val recChannel = parent.channel(scope)

        scope.launch {
            // TODO
        }
        return channel
    }
}