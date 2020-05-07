import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*

fun main() {
    val flows = (0..4).map { n ->
        flow {
            for (i in 1..10)
                emit(i * 5 + n)
        }
    }

    runBlocking {
        val channels = (1..flows.size).map { Channel<Pair<Int, Int>>(100) }
        flows.zip(channels).forEach { (flow, channel) ->
            launch {
                flow.collect {
                    channel.send(it % 5 to it)
                }
                channel.close()
            }
        }

        val pq = PriorityQueue<Pair<Pair<Int, Int>, Int>>(Comparator { p0, p1 -> p0.first.first - p1.first.first })
        for ((i, channel) in channels.withIndex()) {
            pq.add(channel.receive() to i)
        }

        val c = produce {
            while (!pq.isEmpty()) {
                val (v, i) = pq.poll()
                channels[i].receiveOrNull()?.also { pq.add(it to i) }
                send(v)
            }
        }

        val c2 = produce {
            var currentPair: Pair<Int, Int>? = null
            for (pair in c) {
                if (currentPair == null) {
                    currentPair = pair
                }
                if (currentPair.first != pair.first) {
                    send(currentPair)
                    currentPair = pair
                } else {
                    currentPair = pair.first to currentPair.second + pair.second
                }
            }
            if (currentPair != null) {
                send(currentPair)
            }
        }.toList().also { println(it) }
    }
}