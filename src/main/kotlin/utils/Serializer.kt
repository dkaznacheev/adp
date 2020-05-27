package utils

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.withContext
import java.io.*

abstract class Serializer<T>: Serializable {
    abstract fun serialize(o: T): ByteArray
    abstract fun deserialize(s: ByteArray): T
    abstract fun readFileSync(file: File): Iterator<T>
    abstract fun readFile(file: File, scope: CoroutineScope): ReceiveChannel<T>
    abstract suspend fun readFileFlow(file: File, bufferSize: Int, flowCollector: FlowCollector<List<T>>)
    abstract suspend fun writeToFile(recChannel: ReceiveChannel<T>, outFile: File)
    abstract fun writeToOutput(output: Output, o: T)
    abstract fun writeToFile(elements: Iterator<T>, outFile: File)
}

inline fun <reified T, reified U> getPairSerializer(): Serializer<Pair<T, U>> {
    return KryoSerializer(Pair::class.java) as Serializer<Pair<T, U>> // TODO FIX
}

inline fun <reified T> kryoSerializer(kryo: Kryo = Kryo()): KryoSerializer<T> {
    return KryoSerializer(T::class.java, kryo)
}

class KryoSerializer<T>(val clazz: Class<T>, val kryo: Kryo = Kryo()): Serializer<T>() {
    override fun serialize(o: T): ByteArray {
        val ba = ByteArrayOutputStream()
        val output = Output(ba)
        kryo.writeObject(output, o)
        output.flush()
        return ba.toByteArray()
    }

    override fun deserialize(s: ByteArray): T {
        val ba = ByteArrayInputStream(s)
        val input = Input(ba)
        return kryo.readObject(input, clazz)
    }

    override fun readFile(file: File, scope: CoroutineScope): ReceiveChannel<T> {
        return scope.produce {
            withContext(Dispatchers.IO) {
                val input = Input(FileInputStream(file))
                while(!input.eof()) {
                    send(kryo.readObject(input, clazz))
                }
                input.close()
            }
        }
    }

    override fun readFileSync(file: File): Iterator<T> {
        return iterator {
            val input = Input(FileInputStream(file))
            while(!input.eof()) {
                yield(kryo.readObject(input, clazz))
            }
            input.close()
        }
    }

    override fun writeToOutput(output: Output, o: T) {
        kryo.writeObject(output, o)
    }

    override suspend fun writeToFile(recChannel: ReceiveChannel<T>, outFile: File) {
        withContext(Dispatchers.IO) {
            val output = Output(FileOutputStream(outFile))
            for (value in recChannel) {
                kryo.writeObject(output, value)
            }
            output.close()
        }
    }

    override fun writeToFile(elements: Iterator<T>, outFile: File) {
        val output = Output(FileOutputStream(outFile))
        for (value in elements) {
            kryo.writeObject(output, value)
        }
        output.close()
    }

    override suspend fun readFileFlow(file: File, bufferSize: Int, flowCollector: FlowCollector<List<T>>) {
        withContext(Dispatchers.IO) {
            val input = Input(BufferedInputStream(FileInputStream(file)))
            var buffer = mutableListOf<T>()
            while (!input.eof()) {
                if (buffer.size >= bufferSize) {
                    flowCollector.emit(buffer)
                    buffer = mutableListOf()
                }
                buffer.add(kryo.readObject(input, clazz))
            }
            flowCollector.emit(buffer)
            input.close()
        }
    }
}