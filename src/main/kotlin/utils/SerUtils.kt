package utils

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import org.apache.commons.lang3.SerializationUtils
import java.io.*
import java.util.*
import kotlin.reflect.KClass

object SerUtils {
    fun serialize(o: Any?): ByteArray {
        val bos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(bos)
        oos.writeObject(o)
        return bos.toByteArray()
    }

    fun ser(o: Any): ByteArray{
        return SerializationUtils.serialize(o as Serializable)
    }

    fun deserialize(serialized: ByteArray): Any {
        return ObjectInputStream(ByteArrayInputStream(serialized)).readObject()
    }

    fun unwrap(s: String): Any {
        return deserialize(base64decode(s))
    }

    fun wrap(o: Any?): String {
        return base64encode(serialize(o))
    }

    fun toHexString(ba: ByteArray) = ba.joinToString("") { "%02x".format(it) }

    fun base64encode(a: ByteArray): String {
        return String(Base64.getEncoder().encode(a))
    }

    fun base64decode(s: String): ByteArray {
        return Base64.getDecoder().decode(s)
    }

    abstract class Serializer<T>: Serializable {
        abstract fun serialize(o: T): String
        abstract fun deserialize(s: String): T
        abstract fun readFileSync(file: File): Iterator<T>
        abstract fun readFile(file: File, scope: CoroutineScope): ReceiveChannel<T>
        abstract suspend fun writeToFile(recChannel: ReceiveChannel<T>, outFile: File)
        abstract fun writeToFile(elements: Iterable<T>, outFile: File)
    }

    fun getSerializer(c: KClass<*>): Serializer<Any?> {
        return when(c) {
            else -> DefaultSerializer()
        }
    }

    inline fun <reified T, reified U> getPairSerializer(): Serializer<Pair<T, U>> {
        return DefaultSerializer() as Serializer<Pair<T, U>>//PairSerializer(getSerializer<T>(), getSerializer<U>())
    }

    inline fun <reified T> getSerializer(): Serializer<T> {
        return when(T::class) {
            else -> DefaultSerializer() as Serializer<T>
        }
    }

    inline fun <reified T> kryoSerializer(kryo: Kryo = Kryo()): KryoSerializer<T> {
        return KryoSerializer(T::class.java, kryo)
    }

    class KryoSerializer<T>(val clazz: Class<T>, val kryo: Kryo = Kryo()): Serializer<T>() {
        override fun serialize(o: T): String {
            val ba = ByteArrayOutputStream()
            val output = Output(ba)
            kryo.writeObject(output, o)
            output.flush()
            return ba.toByteArray().toString()
        }

        fun serializeBA(o: T): ByteArray {
            val ba = ByteArrayOutputStream()
            val output = Output(ba)
            kryo.writeObject(output, o)
            output.flush()
            return ba.toByteArray()
        }

        fun deserializeBA(s: ByteArray): T {
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

        override suspend fun writeToFile(recChannel: ReceiveChannel<T>, outFile: File) {
            withContext(Dispatchers.IO) {
                val output = Output(FileOutputStream(outFile))
                for (value in recChannel) {
                    kryo.writeObject(output, value)
                }
                output.close()
            }
        }

        override fun writeToFile(elements: Iterable<T>, outFile: File) {
            val output = Output(FileOutputStream(outFile))
            for (value in elements) {
                kryo.writeObject(output, value)
            }
            output.close()
        }

        override fun deserialize(s: String): T {
            val ba = ByteArrayInputStream(s.toByteArray())
            val input = Input(ba)
            return kryo.readObject(input, clazz)
        }
    }

    class DefaultSerializer: Serializer<Any?>() {
        override fun serialize(o: Any?): String {
            return wrap(o)
        }

        override fun deserialize(s: String): Any? {
            return unwrap(s)
        }

        override fun readFile(file: File, scope: CoroutineScope): ReceiveChannel<Any?> {
            return scope.produce {
                withContext(Dispatchers.IO) {
                    for (line in file.bufferedReader().lineSequence()) {
                        send(unwrap(line))
                    }
                }
            }
        }

        override fun readFileSync(file: File): Iterator<Any?> {
            return iterator {
                for (line in file.bufferedReader().lineSequence()) {
                    yield(unwrap(line))
                }
            }
        }

        override suspend fun writeToFile(recChannel: ReceiveChannel<Any?>, outFile: File) {
            withContext(Dispatchers.IO) {
                val writer = outFile.bufferedWriter()
                for (v in recChannel) {
                    writer.write(wrap(v))
                    writer.newLine()
                }
            }
        }

        override fun writeToFile(elements: Iterable<Any?>, outFile: File) {
            val writer = outFile.bufferedWriter()
            for (v in elements) {
                writer.write(wrap(v))
                writer.newLine()
            }
        }
    }
}