package worker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import utils.SerUtils
import java.io.BufferedWriter
import java.io.File
import java.io.OutputStream

class CacheManager(val capacity: Int) {
    var totalSize = 0
    private val cache = hashMapOf<Int, MutableList<Any?>>()
    private val spills = hashMapOf<Int, OutputStream>()
    private val writers = hashMapOf<Int, BufferedWriter>()

    private val cacheDir = File("cache")

    private fun storeCache(id: Int, o: Any?) {
        if (cache.containsKey(id)) {
            cache[id]!!.add(o)
        } else {
            cache[id] = mutableListOf(o)
        }
        println("stored $o in memory")
        totalSize++
    }

    private fun storeSpill(id: Int, o: Any?) {
        if (!writers.containsKey(id)) {
            val os = cacheDir.resolve("cache$id").outputStream()
            spills[id] = os
            writers[id] = os.bufferedWriter()
        }
        writers[id]!!.write(SerUtils.base64encode(SerUtils.serialize(o)))
        writers[id]!!.newLine()
        println("stored $o on disk")
    }

    fun store(id: Int, o: Any?) {
        if (totalSize < capacity) {
            storeCache(id, o)
        } else {
            storeSpill(id, o)
        }
    }

    @ExperimentalCoroutinesApi
    @Suppress("UNCHECKED_CAST")
    fun <T> load(id: Int, scope: CoroutineScope): ReceiveChannel<T> {
        return scope.produce {
            val cached = cache[id] ?: mutableListOf<Any?>()
            for (o in cached) {
                println("loading $o from memory")
                send(o as T)
            }
            val f = cacheDir.resolve("cache$id")
            if (f.exists()) {
                val istream = f.inputStream()
                val lines = istream.bufferedReader().lineSequence()
                for (line in lines) {
                    val o = SerUtils.deserialize(SerUtils.base64decode(line))
                    println("loading $o from disk")
                    send(o as T)
                }
            }
        }
    }

    fun close() {
        writers.forEach { (_, w) -> w.flush()}
        spills.forEach { (_, os) -> os.close()}
    }
}