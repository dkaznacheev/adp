package master
//
//import api.operations.ParallelOperation
//import api.rdd.LocalReduceByKeyRDDImpl
//import api.rdd.RDDImpl
//import kotlinx.coroutines.runBlocking
//import worker.CacheManager
//import utils.SerUtils
//import worker.WorkerContext
//
//class LocalMaster: Master {
//    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
//        return runBlocking {
//            op.toImpl().execute(
//                    this,
//                    WorkerContext(
//                            mutableMapOf(),
//                            CacheManager(100)))
//        }
//    }
//
//    override fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>) {
//
//    }
//
//    override fun <K, T> getReduceByKeyRDDImpl(parent: RDDImpl<Pair<K, T>>, shuffleId: Int, keyComparator: (K, K) -> Int, serializer: SerUtils.Serializer<Pair<K, T>>, f: (T, T) -> T): RDDImpl<Pair<K, T>> {
//        return LocalReduceByKeyRDDImpl(parent, shuffleId, keyComparator, serializer, f)
//    }
//}