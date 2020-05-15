package utils

import kotlinx.coroutines.channels.Channel

class LazyChannel<T> {
    private var value: T? = null
    private val ch = Channel<T>(1000)

    suspend fun get(): T {
        return ch.receiveOrNull() ?: value!!
    }

    suspend fun set(t: T) {
        value = t
        ch.send(t)
        ch.close()
    }
}