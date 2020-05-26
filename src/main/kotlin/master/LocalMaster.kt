package master

import api.operations.ParallelOperation
import api.rdd.LocalReduceByKeyRDDImpl
import api.rdd.LocalSortedRDDImpl
import api.rdd.RDDImpl
import api.rdd.ReduceByKeyGrpcRDDImpl
import kotlinx.coroutines.runBlocking
import utils.NPair
import worker.CacheManager
import utils.SerUtils
import worker.WorkerContext

class LocalMaster: Master {
    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        return runBlocking {
            op.toImpl().execute(
                    this,
                    WorkerContext(
                            mutableMapOf(),
                            CacheManager(100)))
        }
    }

    override fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>) {

    }

    override fun <K, V> getReduceByKeyRDDImpl(parent: RDDImpl<NPair<K, V>>,
                                              shuffleId: Int,
                                              keyComparator: (K, K) -> Int,
                                              tClass: Class<NPair<K, V>>,
                                              f: (V, V) -> V): RDDImpl<NPair<K, V>> {
        return LocalReduceByKeyRDDImpl(parent, shuffleId, keyComparator, tClass, f)
    }

    override fun <T> getSortedRDDImpl(parent: RDDImpl<T>, shuffleId: Int, comparator: (T, T) -> Int, tClass: Class<T>): RDDImpl<T> {
        return LocalSortedRDDImpl(parent, shuffleId, comparator, tClass)
    }
}