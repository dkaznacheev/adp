package utils

class  NPair<A, B>(private val firstN: A? = null, private val secondN: B? = null) {
    operator fun component1(): A = first
    operator fun component2(): B = second

    override fun toString(): String = "$first,$second"

    val first: A
        get() = firstN!!

    val second: B
        get() = secondN!!
}

infix fun <A, B> A.toN(b: B): NPair<A, B> {
    return NPair(this, b)
}