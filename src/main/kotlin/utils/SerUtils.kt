package utils

import org.apache.commons.lang3.SerializationUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
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

    interface Serializer<T>: Serializable {
        fun serialize(o: T): String
        fun deserialize(s: String): T
    }

    fun getSerializer(o: Any): Serializer<Any?> {
        return when(o) {
            is Int -> IntSerializer() as Serializer<Any?>
            is String -> StringSerializer()  as Serializer<Any?>
            else -> DefaultSerializer()
        }
    }

    fun getSerializer(c: KClass<*>): Serializer<Any?> {
        return when(c) {
            Int::class -> IntSerializer() as Serializer<Any?>
            String::class -> StringSerializer()  as Serializer<Any?>
            else -> DefaultSerializer()
        }
    }

    inline fun <reified T, reified U> getPairSerializer(): Serializer<Pair<T, U>> {
        return PairSerializer(getSerializer<T>(), getSerializer<U>())
    }

    inline fun <reified T> getSerializer(): Serializer<T> {
        return when(T::class) {
            Int::class -> IntSerializer() as Serializer<T>
            String::class -> StringSerializer() as Serializer<T>
            else -> DefaultSerializer() as Serializer<T>
        }
    }

    class IntSerializer: Serializer<Int> {
        override fun serialize(o: Int): String {
            return o.toString()
        }

        override fun deserialize(s: String): Int {
            return s.toInt()
        }
    }

    class StringSerializer: Serializer<String> {
        override fun serialize(o: String): String {
            return o
        }

        override fun deserialize(s: String): String {
            return s
        }
    }

    class PairSerializer<T, U>(private val s1: Serializer<T>,
                               private val s2: Serializer<U>) : Serializer<Pair<T, U>> {
        override fun serialize(o: Pair<T, U>): String {
            return s1.serialize(o.first) + SEPARATOR + s2.serialize(o.second)
        }

        override fun deserialize(s: String): Pair<T, U> {
            val parts = s.split(SEPARATOR)
            return s1.deserialize(parts[0]) to s2.deserialize(parts[1])
        }

        companion object {
            const val SEPARATOR = '\u200B'
        }
    }

    class DefaultSerializer: Serializer<Any?> {
        override fun serialize(o: Any?): String {
            return wrap(o)
        }

        override fun deserialize(s: String): Any? {
            return unwrap(s)
        }
    }
}