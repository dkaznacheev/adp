import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

class Master {
    val workers = listOf("w1")

    fun <T> execute(operation: ReduceOperation<T>): T? {
        val serialized = operation.serialize()
        return workers.map { runOnWorker<T>(it, serialized) }
            .map { deserializeResult(it) as T }.reduce(operation.f)
    }

    fun deserializeResult(byteArray: ByteArray): Any {
        val bis = ObjectInputStream(ByteArrayInputStream(byteArray))
        return bis.readObject()
    }

    fun <T> runOnWorker(it: String, serialized: ByteArray): ByteArray {
        val rop = SerUtils.deserialize(serialized) as ReduceOperationImpl<*>
        return rop.executeSerializable()
    }
}