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
//    fun serialize(o: Any?): ByteArray {
//        val kryo = Kryo()
//        return synchronized(kryo) {
//            val ba = ByteArrayOutputStream()
//            val output = Output(ba)
//            kryo.writeObject(output, o)
//            output.flush()
//            ba.toByteArray()
//        }
//    }
//
//    fun deserialize(serialized: ByteArray): Any {
//        val kryo = Kryo()
//        val input = Input(ByteArrayInputStream(serialized))
//        return kryo.readObject(input, Any::class.java)
//    }

    fun base64encode(a: ByteArray): String {
        return String(Base64.getEncoder().encode(a))
    }

    fun base64decode(s: String): ByteArray {
        return Base64.getDecoder().decode(s)
    }
}