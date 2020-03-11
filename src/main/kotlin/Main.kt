import io.ktor.client.HttpClient
import io.ktor.client.request.get
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

fun rddTestAsync() {
    val master = Master()
    val res = SourceRDD(master, "lines.txt").map {
        HttpClient().get<String>(it)
    }.map {
        it[14].toString()
    }.reduce { a, b ->
        a + b
    }
    println(res)
}

fun rddTestAsync2() {
    val master = Master()
    SourceRDD(master, "lines.txt").map {
        HttpClient().get<String>(it)
    }.map{
        it[14]
    }.saveAsObject("result")
}

fun main(args: Array<String>) {
    if (args.isNotEmpty() && args[0] == "worker") {
        Worker(args[1].toInt()).start()
    } else {
        rddTestAsync()
    }
}
