import api.rdd.*
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import master.GrpcMaster
import repl.REPLInterpreter
//import master.LocalMaster
import rowdata.ColumnDataType
import utils.SerUtils
import utils.toN
import worker.Worker
import java.io.File
import java.io.FileOutputStream
import kotlin.math.abs
import kotlin.random.Random
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

//fun reduceByKeyGrpcTest() {
//    val workers = File("workers.conf").readLines()
//    val master = GrpcMaster(8099, workers)
//    fileRdd<String>(master, "tmp.csv")
//        .map {
//            val parts = it.split(",")
//            parts[0] to parts[1].toInt()
//        }
//        .reduceByKey { a, b -> a + b }
//        .saveAsObject(SerUtils.getPairSerializer<String, Int>(),"shuffled.txt")
//}
//
//fun reduceGrpcFileTest() {
//    val workers = File("workers.conf").readLines()
//    val master = GrpcMaster(8099, workers)
//    fileRdd<String>(master,"tmp.csv")
//            .map { it.split(",")[1].toInt() }
//            .reduce { a, b -> a + b}
//            .also { println(it) }
//}
//
//fun reduceHTTPTest() {
//    val workers = File("workers.conf").readLines()
//    val master = GrpcMaster(8099, workers)
//    fileRdd<Int>(master,"numbers.txt")
//            .mapHTTP {
//                val t = get<String>("http://localhost:8085/echo?value=$it")
//                t.length
//            }
//            .reduce { a, b -> a + b}
//            .also { println(it) }
//}
//
//fun saveAsObjectInlineTest() {
//    val workers = File("workers.conf").readLines()
//    val master = GrpcMaster(8099, workers)
//    fileRdd<Int>(master,"numbers.txt")
//            .map { it.toString() }
//            .saveAsObject("file.txt")
//}


fun multiWorkerTest(port: Int) {
    val workers = File("workers.conf").readLines()
    val master = GrpcMaster(port, workers)
    LinesRDD(master, "tmp.csv")
        .map {
            val parts = it.split(",")
            parts[0] toN parts[1].toInt()
        }
        .reduceByKey { a, b -> a + b }
        .map { (a, b) -> "$a: $b"}
        .show()
}

fun singleWorkerTest() {
    val workers = File("workers.conf").readLines().take(1)
    val master = GrpcMaster(8099, workers)
    LinesRDD(master, "tmp.csv")
        .map {
            val parts = it.split(",")
            parts[1].toInt()
        }
        .sorted()
        .show()
}

class Main {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            if (args.isEmpty()) {
                println("no args provided: expected master/worker <port>/repl")
                return
            }
            when (args[0]) {
                "worker" -> Worker(args[1].toInt()).startRPC()
                "master" -> {
                    measureTimeMillis {
                        multiWorkerTest(args[1].toInt())
                    }.also { println("completed in $it ms") }
                }
                "repl" -> {
                    REPLInterpreter.main(args)
                }

                "generate" -> {
                    val n = args[2].toInt()
                    val random = Random(System.currentTimeMillis())
                    File(args[1]).bufferedWriter().use {
                        for (i in 1..n) {
                            it.write((abs(random.nextInt()) % 10000).toString())
                            it.newLine()
                        }
                    }
                }
                else -> println("no such mode ${args[1]}")
            }
        }
    }
}
