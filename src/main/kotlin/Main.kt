import api.rdd.*
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import master.GrpcMaster
import master.LocalMaster
import rowdata.ColumnDataType
import utils.SerUtils
import worker.Worker
import java.io.File
import kotlin.system.measureTimeMillis

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


fun reduceByKeyGrpcTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8099, workers)
    fileRdd<String>(master, "tmp.csv")
        .map {
            val parts = it.split(",")
            parts[0] to parts[1].toInt()
        }
        .reduceByKey { a, b -> a + b }
        .saveAsObject(SerUtils.getPairSerializer<String, Int>(),"shuffled.txt")
}

fun reduceGrpcFileTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8099, workers)
    fileRdd<String>(master,"tmp.csv")
            .map { it.split(",")[1].toInt() }
            .reduce { a, b -> a + b}
            .also { println(it) }
}

fun reduceHTTPTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8099, workers)
    fileRdd<Int>(master,"numbers.txt")
            .mapHTTP {
                val t = get<String>("http://localhost:8085/echo?value=$it")
                t.length
            }
            .reduce { a, b -> a + b}
            .also { println(it) }
}

fun saveAsObjectInlineTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8099, workers)
    fileRdd<Int>(master,"numbers.txt")
            .map { it.toString() }
            .saveAsObject("file.txt")
}

fun serTest() {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(8099, workers)
}

fun main(args: Array<String>) {
    if (args.isNotEmpty() && args[0] == "worker") {
        Worker(args[1].toInt()).startRPC()
    } else {
        //TestService(8085, 10L).start()
        measureTimeMillis {
            reduceByKeyGrpcTest()
        }.also { println(it) }
    }
}
