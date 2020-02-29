abstract class RDD<T>(val master: Master) {
    fun <R> map(f: (T) -> R): RDD<R> {
        return MappedRDD(this, f)
    }

    fun reduce(clazz: Class<T>, f: (T, T) -> T): T? {
        return master.execute(ReduceOperation(this, f, clazz))
    }

    abstract fun toImpl(): RDDImpl<T>
}

class SourceRDD(master: Master, val filename: String) : RDD<String>(master) {
    override fun toImpl(): RDDImpl<String> {
        return SourceRDDImpl(filename)
    }
}

class MappedRDD<T, R>(val parent: RDD<T>, val f: (T) -> R): RDD<R>(parent.master) {
    override fun toImpl(): RDDImpl<R> {
        return MappedRDDImpl(parent.toImpl(), f)
    }
}

abstract class ParallelOperation<T, U>(val rdd: RDD<T>) {
    //abstract fun execute(): U
}

class ReduceOperation<T>(rdd: RDD<T>, val f: (T, T) -> T, val clazz: Class<T>): ParallelOperation<T, T>(rdd) {
    fun serialize(): ByteArray {
        return SerUtils.serialize(ReduceOperationImpl(rdd.toImpl(), f))
    }
}