import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce

abstract class RDDAsync<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDDAsync<R> {
        return MappedRDDAsync(this, f)
    }

    fun reduce(f: (T, T) -> T): T? {
        return master.executeAsync(ReduceOperationAsync(this, f))
    }

    fun saveAsObject(name: String) {
        master.executeAsync(SaveAsObjectOperationlAsync(this, name))
    }

    abstract fun toImpl(): RDDImplAsync<T>
}

class SourceRDDAsync(master: Master, val filename: String) : RDDAsync<String>(master) {
    override fun toImpl(): RDDImplAsync<String> {
        return SourceRDDImplAsync(filename)
    }
}

class MappedRDDAsync<T, R>(val parent: RDDAsync<T>, val f: suspend (T) -> R): RDDAsync<R>(parent.master) {
    override fun toImpl(): RDDImplAsync<R> {
        return MappedRDDImplAsync(parent.toImpl(), f)
    }
}

abstract class ParallelOperationAsync<T, R> (val rdd: RDDAsync<T>) {
    abstract fun serialize(): ByteArray
    abstract suspend fun consumeParts(channel: ReceiveChannel<R>): R
}

class SaveAsObjectOperationlAsync<T>(rdd: RDDAsync<T>, val name: String): ParallelOperationAsync<T, Byte>(rdd) {
    override fun serialize(): ByteArray {
        return SerUtils.serialize(SaveAsObjectOperationImplAsync(rdd.toImpl(), name))
    }

    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS}
    }
}

class ReduceOperationAsync<T>(rdd: RDDAsync<T>, val f: (T, T) -> T): ParallelOperationAsync<T, T>(rdd) {
    override fun serialize(): ByteArray {
        return SerUtils.serialize(ReduceOperationImplAsync(rdd.toImpl(), f))
    }

    override suspend fun consumeParts(channel: ReceiveChannel<T>): T {
        return channel.reduce(f)
    }
}

class ReduceByKeyOperationAsync<K, T>(val parent: RDDAsync<Pair<K, T>>, val f: (T, T) -> T): RDDAsync<Pair<K, T>>(parent.master) {
    override fun toImpl(): RDDImplAsync<Pair<K, T>> {
        return ReduceByKeyOperationImplAsync(parent.toImpl(), f)
    }

}
