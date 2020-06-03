import api.rdd.*
import master.LocalMaster
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.toN

class ADPLocalTest {
    @Test
    fun simpleTest() {
        val master = LocalMaster()
        val tmpFile = createTempFile()
        tmpFile.bufferedWriter().use {
            for (i in 1..100) {
                it.write(i.toString())
                it.newLine()
            }
        }
        val sum = LinesRDD(master, tmpFile.absolutePath).map { it.toInt() }.reduce(0) { a, b -> a + b }
        assertEquals((1..100).sum(), sum)
    }


    @Test
    fun reduceByKeyTest() {
        val master = LocalMaster()
        val tmpFile = createTempFile()
        tmpFile.bufferedWriter().use {
            for (i in 1..100) {
                val key = 'a' + i % 10
                it.write("$key,$i")
                it.newLine()
            }
        }
        val sumByKeys = LinesRDD(master, tmpFile.absolutePath)
                .map {
                    val parts = it.split(",")
                    parts[0] toN  parts[1].toInt()
                }
                .reduceByKey { a, b -> a + b }.saveAsText("result.txt")

        println(sumByKeys)
    }
}