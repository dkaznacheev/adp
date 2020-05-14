package master

import api.operations.ParallelOperation
import api.rdd.RDDImpl
import utils.SerUtils

interface Master {
    fun <T, R> execute(op: ParallelOperation<T, R>): R

    fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>)

    fun <K, T> getReduceByKeyRDDImpl(parent: RDDImpl<Pair<K, T>>,
                                     shuffleId: Int,
                                     keyComparator: Comparator<K>,
                                     serializer: SerUtils.Serializer<Pair<K, T>>,
                                     f: (T, T) -> T): RDDImpl<Pair<K, T>>
}

