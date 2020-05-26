package master

import api.operations.ParallelOperation
import api.rdd.RDDImpl
import utils.NPair

interface Master {
    fun <T, R> execute(op: ParallelOperation<T, R>): R

    fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>)

    fun <K, T> getReduceByKeyRDDImpl(parent: RDDImpl<NPair<K, T>>,
                                     shuffleId: Int,
                                     keyComparator: (K, K) -> Int,
                                     tClass: Class<NPair<K, T>>,
                                     f: (T, T) -> T): RDDImpl<NPair<K, T>>

    fun <T> getSortedRDDImpl(parent: RDDImpl<T>,
                                     shuffleId: Int,
                                     comparator: (T, T) -> Int,
                                     tClass: Class<T>): RDDImpl<T>
}

