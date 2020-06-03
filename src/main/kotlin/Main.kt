import api.rdd.*
import io.ktor.client.request.get
import master.GrpcMaster
import repl.REPLInterpreter
import utils.toN
import worker.Worker
import java.io.File
import kotlin.math.abs
import kotlin.random.Random
import kotlin.system.measureTimeMillis


fun numberCount(filename: String, workerNum: Int, port: Int) {
    val workers = File("workers.conf").readLines().take(workerNum)
    val master = GrpcMaster(port, workers)
    LinesRDD(master, filename)
        .map {
            it.toInt() toN 1
        }
        .reduceByKey { a, b -> a + b }
        .map { (a, b) -> "$a: $b"}
        .saveAsText("counts.txt")
}

fun httpMap(filename: String, workerNum: Int, port: Int) {
    val workers = File("workers.conf").readLines().take(workerNum)
    val master = GrpcMaster(port, workers)
    LinesRDD(master, filename)
            .mapHTTP {
                get<String>("https://postman-echo.com/get?value=$it")[18].toInt() - 48
            }
            .reduce(0) { a, b -> (a + b) % 10000 }.also{ println(it) }
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
                        val workerNum = args[1].toInt()
                        val filename = "numbers$workerNum.txt"
                        val port = 8099
                        when (args[2]) {
                            "http" -> httpMap(filename, workerNum, port)
                            "count" -> numberCount(filename, workerNum, port)
                        }
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
