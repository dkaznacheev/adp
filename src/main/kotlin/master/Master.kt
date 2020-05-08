package master

import api.operations.ParallelOperation

interface Master {
    fun <T, R> execute(op: ParallelOperation<T, R>): R

    fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>)
}

