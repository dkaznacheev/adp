package api.operations

import api.MAX_CAP
import api.rdd.RDD
import api.rdd.RDDImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch

class ReduceByKeyOperation<K, T>(val parent: RDD<Pair<K, T>>, val f: (T, T) -> T): RDD<Pair<K, T>>(parent.master) {
    override fun toImpl(): RDDImpl<Pair<K, T>> {
        return ReduceByKeyOperationImpl(parent.toImpl(), f)
    }
}

class ReduceByKeyOperationImpl<K, T>(val parent: RDDImpl<Pair<K, T>>, val f: (T, T) -> T): RDDImpl<Pair<K, T>>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<Pair<K, T>> {
        val channel = Channel<Pair<K, T>>(MAX_CAP)
        val recChannel = parent.channel(scope)

        scope.launch {
            // TODO
        }
        return channel
    }
}