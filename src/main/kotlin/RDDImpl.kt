import java.io.File
import java.io.Serializable

abstract class RDDImpl<T>: Iterable<T> {
}

class MappedRDDImpl<T, R>(val parent: RDDImpl<T>, val f: (T) -> R): RDDImpl<R>(), Serializable {
    override fun iterator(): Iterator<R> {
        return object : Iterator<R> {
            val parentIt = parent.iterator()
            override fun hasNext(): Boolean {
                return parentIt.hasNext()
            }

            override fun next(): R {
                return f(parentIt.next())
            }
        }
    }
}

class SourceRDDImpl(val filename: String): RDDImpl<String>(), Serializable {
    override fun iterator(): Iterator<String> {
        return File(filename).bufferedReader().lineSequence().iterator()
    }
}

class ReduceOperationImpl<T>(val rdd: RDDImpl<T>, val f: (T, T) -> T): Serializable {
    fun execute(): T {
        return rdd.iterator().asSequence().reduce(f)
    }

    fun executeSerializable(): ByteArray {
        val result = execute() as Serializable
        return SerUtils.serialize(result)
    }
}
