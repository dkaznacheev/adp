import api.rdd.*
import master.LocalMaster
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.toN
import java.io.File

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
        val localDir = File("local")
        if (!localDir.exists()) {
            localDir.mkdir()
        }
        tmpFile.bufferedWriter().use {
            for (i in 1..100) {
                val key = 'a' + i % 3
                it.write("$key,$i")
                it.newLine()
            }
        }
        val sumByKeys = LinesRDD(master, tmpFile.absolutePath)
                .map {
                    val parts = it.split(",")
                    parts[0] toN  parts[1].toInt()
                }
                .reduceByKey { a, b ->
                    a + b
                }.mapSync { it.toString() }
                .reduce("") { a, b -> a + "\n" + b }

        assertEquals("a,1683\n" +
                     "b,1717\n" +
                     "c,1650", sumByKeys)
    }
}