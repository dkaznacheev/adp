package repl

import api.rdd.LinesRDD
import api.rdd.RDD
import api.rdd.fileRdd
import api.rdd.map
import master.GrpcMaster
import master.LocalMaster
import master.Master
import org.jetbrains.kotlin.cli.common.repl.AggregatedReplStageState
import org.jetbrains.kotlin.cli.common.repl.ReplCodeLine
import org.jetbrains.kotlin.cli.common.repl.ReplCompileResult
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.script.experimental.annotations.KotlinScript
import kotlin.script.experimental.api.ScriptCompilationConfiguration
import kotlin.script.experimental.api.ScriptEvaluationConfiguration
import kotlin.script.experimental.api.implicitReceivers
import kotlin.script.experimental.jvm.dependenciesFromCurrentContext
import kotlin.script.experimental.jvm.jvm
import kotlin.script.experimental.jvm.util.classpathFromClassloader
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

    class ReceiverHolder(val master: Master) {
        inline fun <reified T> fileRDD(s: String): RDD<T> {
            return fileRdd(master, s)
        }

        fun linesRDD(s: String): RDD<String> {
            return LinesRDD(master, s)
        }
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {

            val master = if (args[1] == "local") {
                LocalMaster()
            } else {
                val workers = File("workers.conf").readLines()
                GrpcMaster(args[2].toInt(), workers)
            }
            val holder = ReceiverHolder(master)

            val compilationConf = createJvmCompilationConfigurationFromTemplate<SimpleScript> {
                jvm { dependenciesFromCurrentContext(wholeClasspath = true) }
                implicitReceivers(ReceiverHolder::class)
            }
            val evaluationConf = createJvmEvaluationConfigurationFromTemplate<SimpleScript> {
                implicitReceivers(holder)
            }

            val repl = REPLInterpreter(compilationConf, evaluationConf)
            repl.eval("import api.rdd.*")
            print("> ")
            var builder = StringBuilder("\n")
            System.`in`.bufferedReader().lineSequence().forEach {
                if (it.endsWith("\\")) {
                    builder.append(it.trimEnd('\\'))
                    print("... ")
                }
                else {
                    builder.append(it)
                    println(repl.eval(it))
                    print("> ")
                }
            }
        }
    }
}