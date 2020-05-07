import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class GrpcShuffleManager {
    fun blockFor(shuffleId: Int, worker: Int): Flow<Adp.Value> {
        return flow {

        }
    }

    fun <T> writeAndBroadcast(scope: CoroutineScope, recChannel: ReceiveChannel<T>, shuffleId: Int) {

    }

    fun <T> readMerged(scope: CoroutineScope, shuffleId: Int): ReceiveChannel<T> {
        return Channel(10)
    }
}