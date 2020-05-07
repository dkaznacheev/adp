package utils

import org.apache.commons.lang3.SerializationUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.util.*

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
}