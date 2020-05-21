import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*
import kotlin.Comparator


fun main() {
    val flows = (0..4).map { n ->
        flow {
            for (i in 1..10)
                emit(i * 5 + n)
        }
    }

    runBlocking {
        val channels = flows.map { flow ->
            produce {
                flow.collect { send(it) }
            }
        }

        val pq = PriorityQueue<Pair<Int, Int>>(Comparator { p0, p1 -> p0.first - p1.first })
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

        c.consumeEach { println(it) }

//        val c2 = produce {
//            var currentPair: Pair<Int, Int>? = null
//            for (pair in c) {
//                if (currentPair == null) {
//                    currentPair = pair
//                }
//                if (currentPair.first != pair.first) {
//                    send(currentPair)
//                    currentPair = pair
//                } else {
//                    currentPair = pair.first to currentPair.second + pair.second
//                }
//            }
//            if (currentPair != null) {
//                send(currentPair)
//            }
//        }.toList().also { println(it) }
    }
}