package shuffle

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import utils.ExternalSorter
import utils.KryoSerializer
import worker.WorkerContext

class LocalShuffleManager<T>(val ctx: WorkerContext,
                             shuffleId: Int,
                             private val comparator: Comparator<T>,
                             val tClass: Class<T>): WorkerShuffleManager<T>  {
    private val shuffleDir = createTempDir().resolve("local/shuffle$shuffleId")

    @ExperimentalCoroutinesApi
    override suspend fun writeAndBroadcast(scope: CoroutineScope, recChannel: ReceiveChannel<T>) {
        if (!shuffleDir.exists()) {
            shuffleDir.mkdirs()
        }
        val sorter = ExternalSorter(shuffleDir, comparator, tClass)
        sorter.sortAndWrite(scope, recChannel)
    }

    @ExperimentalCoroutinesApi
    override fun readMerged(scope: CoroutineScope): ReceiveChannel<T> {
        return scope.produce {
            val block = shuffleDir.resolve("block")
            val serializer = KryoSerializer(tClass)
            for (element in serializer.readFile(block, this))
                send(element)
        }
    }
}