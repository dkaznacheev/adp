package shuffle

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel

interface WorkerShuffleManager<T> {
    suspend fun writeAndBroadcast(
            scope: CoroutineScope,
            recChannel: ReceiveChannel<T>)

    fun readMerged(scope: CoroutineScope): ReceiveChannel<T>
}