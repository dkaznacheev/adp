package rdd

import Master
import utils.SerUtils
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.reduce
import rowdata.ColumnDataType
import rowdata.Row

abstract class RDD<T>(val master: Master) {
    fun <R> map(f: suspend (T) -> R): RDD<R> {
        return MappedRDD(this, f)
    }

    fun reduce(f: (T, T) -> T): T? {
        return master.execute(ReduceOperation(this, f))
    }

    fun saveAsObject(name: String) {
        master.execute(SaveAsObjectOperation(this, name))
    }

    abstract fun toImpl(): RDDImpl<T>
}

class SourceRDD(master: Master, val filename: String) : RDD<String>(master) {
    override fun toImpl(): RDDImpl<String> {
        return SourceRDDImpl(filename)
    }
}

class CsvRDD(master: Master,
             val filename: String,
             val hasHeader: Boolean,
             val separator: String = ",",
             val types: List<ColumnDataType>? = null): RDD<Row>(master) {
    override fun toImpl(): RDDImpl<Row> {
        return CsvRDDImpl(filename, hasHeader, separator, types)
    }
}

class MappedRDD<T, R>(val parent: RDD<T>, val f: suspend (T) -> R): RDD<R>(parent.master) {
    override fun toImpl(): RDDImpl<R> {
        return MappedRDDImpl(parent.toImpl(), f)
    }
}

abstract class ParallelOperation<T, R> (val rdd: RDD<T>) {
    abstract fun toImpl(): ParallelOperationImpl<T, R>
    abstract suspend fun consumeParts(channel: ReceiveChannel<R>): R

    open fun serialize(): ByteArray {
        return SerUtils.serialize(toImpl())
    }
}

class SaveAsCsvOperation<T>(rdd: RDD<T>, val name: String): ParallelOperation<T, Byte>(rdd) {
    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }

    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsCsvOperationImpl(
            rdd.toImpl(),
            name
        )
    }
}

class SaveAsObjectOperation<T>(rdd: RDD<T>, val name: String): ParallelOperation<T, Byte>(rdd) {
    override fun toImpl(): ParallelOperationImpl<T, Byte> {
        return SaveAsCsvOperationImpl(
            rdd.toImpl(),
            name
        )
    }

    override suspend fun consumeParts(channel: ReceiveChannel<Byte>): Byte {
        return channel.reduce {_, _ -> SUCCESS }
    }
}

class ReduceOperation<T>(rdd: RDD<T>, val f: (T, T) -> T): ParallelOperation<T, T>(rdd) {
    override suspend fun consumeParts(channel: ReceiveChannel<T>): T {
        return channel.reduce(f)
    }

    override fun toImpl(): ParallelOperationImpl<T, T> {
        return ReduceOperationImpl(rdd.toImpl(), f)
    }
}

class ReduceByKeyOperation<K, T>(val parent: RDD<Pair<K, T>>, val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return ReduceByKeyOperationImpl(parent.toImpl(), f)
    }
}
