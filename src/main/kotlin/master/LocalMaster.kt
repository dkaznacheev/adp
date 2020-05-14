package master

import api.operations.ParallelOperation
import kotlinx.coroutines.runBlocking
import worker.CacheManager
import worker.GrpcShuffleManager
import worker.ShuffleManager
import worker.WorkerContext

class LocalMaster: Master {
    override fun <T, R> execute(op: ParallelOperation<T, R>): R {
        return runBlocking {
            op.toImpl().execute(
                    this,
                    WorkerContext(
                            ShuffleManager(0, listOf()),
                            mutableMapOf(),
                            CacheManager(100)))
        }
    }

    override fun <K> addShuffleManager(masterShuffleManager: MasterShuffleManager<K>) {

    }
}