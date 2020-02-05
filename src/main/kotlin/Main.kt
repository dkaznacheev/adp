import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

fun sertest() {
    val f: suspend (Int) -> Int = {
        println("!!!")
        it * it
    }
    val ba = SerUtils.serialize(f)
    val g = SerUtils.deserialize(ba) as (suspend ((Int) -> Int))

    GlobalScope.launch {
        println(g(100))
    }
}

fun rddtest() {
    val master = Master()
    val res = SourceRDD(master, "lines.txt").map { it }.reduce(String::class.java) { a, b -> a + b }
    println(res)
}

fun main() {
    sertest()
}
