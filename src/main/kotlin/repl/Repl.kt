package repl

import org.jetbrains.kotlin.cli.common.repl.AggregatedReplStageState
import org.jetbrains.kotlin.cli.common.repl.ReplCodeLine
import org.jetbrains.kotlin.cli.common.repl.ReplCompileResult
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.script.experimental.annotations.KotlinScript
import kotlin.script.experimental.api.ScriptCompilationConfiguration
import kotlin.script.experimental.api.ScriptEvaluationConfiguration
import kotlin.script.experimental.jvm.dependenciesFromCurrentContext
import kotlin.script.experimental.jvm.jvm
import kotlin.script.experimental.jvmhost.createJvmCompilationConfigurationFromTemplate
import kotlin.script.experimental.jvmhost.createJvmEvaluationConfigurationFromTemplate
import kotlin.script.experimental.jvmhost.repl.JvmReplCompiler
import kotlin.script.experimental.jvmhost.repl.JvmReplEvaluator


@KotlinScript(fileExtension = "kts")
abstract class SimpleScript

class REPLInterpreter(
    compilationConfiguration: ScriptCompilationConfiguration,
    evaluationConfiguration: ScriptEvaluationConfiguration
) {
    private val compiler = JvmReplCompiler(compilationConfiguration)
    private val evaluator = JvmReplEvaluator(evaluationConfiguration)

    private val stateLock = ReentrantReadWriteLock()
    private val state = AggregatedReplStageState(compiler.createState(stateLock), evaluator.createState(stateLock), stateLock)
    private val counter = AtomicInteger(0)

    fun eval(code: String): String? {
        val compileResult = compiler.compile(state, ReplCodeLine(counter.getAndIncrement(), 0, code))
        return when (compileResult) {
            is ReplCompileResult.CompiledClasses -> {
                val res = evaluator.eval(state, compileResult, null)
                res.toString()
            }
            is ReplCompileResult.Incomplete -> {
                "error: incomplete"
            }
            is ReplCompileResult.Error -> {
                "${compileResult.message}\nlocation: ${compileResult.location}"
            }
        }
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            val compilationConf = createJvmCompilationConfigurationFromTemplate<SimpleScript> {
                jvm { dependenciesFromCurrentContext(wholeClasspath = true) }
            }
            val evaluationConf = createJvmEvaluationConfigurationFromTemplate<SimpleScript> {
            }

            val repl = REPLInterpreter(compilationConf, evaluationConf)
            print("> ")
            System.`in`.bufferedReader().lineSequence().forEach {
                println(repl.eval(it))
                print("> ")
            }
        }
    }
}