import kotlinx.coroutines.ExperimentalCoroutinesApi

abstract class RDDAsync<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDDAsync<R> {
        return MappedRDDAsync(this, f)
    }

    fun reduce(clazz: Class<T>, f: (T, T) -> T): T? {
        return master.executeAsync(ReduceOperationAsync(this, f, clazz))
    }

    abstract fun toImpl(): RDDImplAsync<T>
}

class SourceRDDAsync(master: Master, val filename: String) : RDDAsync<String>(master) {
    @ExperimentalCoroutinesApi
    override fun toImpl(): RDDImplAsync<String> {
        return SourceRDDImplAsync(filename)
    }
}

class MappedRDDAsync<T, R>(val parent: RDDAsync<T>, val f: suspend (T) -> R): RDDAsync<R>(parent.master) {
    override fun toImpl(): RDDImplAsync<R> {
        return MappedRDDImplAsync(parent.toImpl(), f)
    }
}

abstract class ParallelOperationAsync<T, U>(val rdd: RDDAsync<T>) {
    //abstract fun execute(): U
}

class ReduceOperationAsync<T>(rdd: RDDAsync<T>, val f: (T, T) -> T, val clazz: Class<T>): ParallelOperationAsync<T, T>(rdd) {
    fun serialize(): ByteArray {
        return SerUtils.serialize(ReduceOperationImplAsync(rdd.toImpl(), f))
    }
}