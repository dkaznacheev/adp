package api.rdd

import Master
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import rowdata.ColumnDataType
import rowdata.MetaData
import rowdata.Row
import java.io.File

class CsvRDD(master: Master,
             val filename: String,
             val hasHeader: Boolean,
             val separator: String = ",",
             val types: List<ColumnDataType>? = null): RDD<Row>(master) {
    override fun toImpl(): RDDImpl<Row> {
        return CsvRDDImpl(filename, hasHeader, separator, types)
    }
}

class CsvRDDImpl(val filename: String,
                 val hasHeader: Boolean,
                 val separator: String,
                 val types: List<ColumnDataType>?): RDDImpl<Row>() {
    override fun channel(scope: CoroutineScope): ReceiveChannel<Row> {
        val linesReader = File(filename).bufferedReader()
        val firstLine = linesReader.readLine()
        val metaData = MetaData.parseMeta(firstLine, hasHeader, separator, types)
        val lines = if (hasHeader) {
            linesReader.lineSequence()
        } else {
            sequenceOf(firstLine) + linesReader.lineSequence()
        }
        return scope.produce {
            for (line in lines) {
                send(metaData.parseRow(line))
            }
        }
    }

}