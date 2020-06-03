import api.rdd.*
import io.ktor.client.request.get
import master.GrpcMaster
import org.junit.Test
import utils.toN
import java.io.File

class ADPMultiWorkerTest {
    @Test
    fun httpMap(filename: String, workerNum: Int, port: Int) {
        val workers = File("workers.conf").readLines().take(workerNum)
        val master = GrpcMaster(port, workers)
        LinesRDD(master, filename)
            .mapHTTP {
                get<String>("https://postman-echo.com/get?value=$it")[18].toInt() - 48
            }
            .reduce(0) { a, b -> (a + b) % 10000 }.also{ println(it) }
    }

    @Test
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
}