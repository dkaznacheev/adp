import io.ktor.client.HttpClient
import io.ktor.client.request.get
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import api.rdd.CsvRDD
import api.rdd.StringRDD
import utils.SerUtils

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
    val master = MultiWorkerMaster()
    val res = StringRDD(master, "lines.txt").map {
        HttpClient().get<String>(it)
    }.map {
        it[14].toString()
    }.reduce { a, b ->
        a + b
    }
    println(res)
}

fun rddTestAsync2() {
    val master = MultiWorkerMaster()
    StringRDD(master, "lines.txt").map {
        HttpClient().get<String>(it)
    }.map{
        it[14]
    }.saveAsObject("result")
}

fun rddTestLocal() {
    val master = LocalMaster()
    StringRDD(master, "workers/worker1/lines.txt").map {
        HttpClient().get<String>(it)
    }.map{
        it[14].toString()
    }.reduce {a, b -> a + b}.also { println(it) }
}

fun rddTestLocalCsv() {
    val master = LocalMaster()
    CsvRDD(
        master,
        "tmp.csv",
        false
    )
        .map {
            it.getString("line0")!! + it.getString("line1")!!
        }.reduce { a, b -> a + b }.also(::println)
}

fun main(args: Array<String>) {
    if (args.isNotEmpty() && args[0] == "worker") {
        Worker(args[1].toInt()).start()
    } else {
        rddTestLocalCsv()
    }
}
