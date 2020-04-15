package rowdata

import rowdata.ColumnDataType.*
import java.util.NoSuchElementException

enum class ColumnDataType {
    STRING,
    INT,
    DOUBLE,
    BOOLEAN
}

data class ColumnDesc(val name: String, val type: ColumnDataType)

class MetaData(val schema: List<ColumnDesc>, val separator: String = ",") {
    private val colMappings = schema.mapIndexed { i, columnDesc ->
        columnDesc.name to i
    }.toMap()

    fun indexOf(col: String): Int? = colMappings[col]

    fun parseRow(s: String): Row {
        val values = s.split(separator)
            .zip(schema)
            .map { (str, desc) ->
                when (desc.type) {
                    STRING -> str
                    INT -> str.toInt()
                    DOUBLE -> str.toDouble()
                    BOOLEAN -> str.toBoolean()
                }
            }
        return Row(this, values)
    }

    companion object {
        fun parseMeta(firstLine: String, hasHeader: Boolean, separator: String, types: List<ColumnDataType>?): MetaData {
            val tokens = firstLine.split(separator)
            val actualTypes = types ?: generateSequence { STRING }.take(tokens.size).toList()
            val names = if (hasHeader) {
                tokens
            } else {
                (tokens.indices).map { "line$it" }.toList()
            }
            return MetaData(
                names.zip(actualTypes).map { (name, type) -> ColumnDesc(name, type) },
                separator
            )
        }
    }
}

class Row(private val meta: MetaData, private val values: List<Any?>) {
    fun get(col: String): Any? {
        val i = meta.indexOf(col) ?: throw NoSuchElementException(col)
        return values[i]
    }

    operator fun get(i: Int): Any? {
        return values[i]
    }

    override fun toString(): String {
        return values.joinToString(meta.separator) { it.toString() }
    }

    fun getInt(col: String): Int? {
        return get(col) as Int?
    }

    fun getString(col: String): String? {
        return get(col) as String?
    }

    fun getDouble(col: String): Double? {
        return get(col) as Double?
    }

    fun getBoolean(col: String): Boolean? {
        return get(col) as Boolean?
    }
}