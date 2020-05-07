import api.rdd.*
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import rowdata.ColumnDataType
import utils.SerUtils
import java.io.File

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
    val master = MultiWorkerMaster(listOf(8080, 8081))
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
    val master = MultiWorkerMaster(listOf(8080, 8081))
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
            it.getString("col0")!! + it.getString("col1")!!
        }.reduce { a, b -> a + b }.also(::println)
}

fun saveToCsvTest() {
    val master = LocalMaster()
    CsvRDD(
        master,
        "tmp.csv",
        true
    ).map {
        it.getString("col0")!!
    }.saveAsCSV("res.csv")
}

fun reduceByKeyLocalTest() {
    val master = LocalMaster()
    CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).map {
        it.getString("col0")!! to it.getInt("col1")!!
    }.reduceByKey { a, b -> a + b}
        .map { it.toString() }
        .reduce { a, b -> a + b }
        .also { println(it) }
}

fun filterLocal() {
    val master = LocalMaster()
    CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).filter {
        it.getInt("col1")!! % 2 == 0
    }.show()
}


fun reduceByKeyMultiNodeTest() {
    val master = MultiWorkerMaster(listOf(8080, 8081))
    CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).map {
        it.getString("col0")!! to it.getInt("col1")!!
    }.reduceByKey { a, b -> a + b}
        .show()
}

fun cacheMultiNodeTest() {
    val master = MultiWorkerMaster(listOf(8080, 8081))
    val id = CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).map {
        it.getInt("col1")!!
    }.cache()
    CachedRDD<Int>(master, id).reduce {a, b -> a + b}.also { println(it) }
}

fun serviceTest() {
    val master = MultiWorkerMaster(listOf(8080, 8081))
    CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).map {
        it.getInt("col1")!!
    }.map {
        HttpClient().get<String>("http://localhost:8085/echo?value=$it")
    }.show()
}

fun mapSyncTest() {
    val master = MultiWorkerMaster(listOf(8080, 8081))
    CsvRDD(
        master,
        "tmp.csv",
        true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT)
    ).mapSync {
        it.getInt("col1")!!
    }.show()
}

fun reduceGrpcTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8090, workers)
    CsvRDD(master,
    "tmp.csv",
    true,
        types = listOf(ColumnDataType.STRING, ColumnDataType.INT))
        .map { it.getInt("col1")!! }
        .reduce { a, b -> a + b}
        .also { println(it) }
}

fun main(args: Array<String>) {
    if (args.isNotEmpty() && args[0] == "worker") {
        Worker(args[1].toInt()).startRPC()
    } else {
        reduceGrpcTest()
    }
}
