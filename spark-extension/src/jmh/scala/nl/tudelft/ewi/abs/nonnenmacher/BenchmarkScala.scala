package nl.tudelft.ewi.abs.nonnenmacher

package nl.tudelft.ewi.abs.nonnenmacher

import org.openjdk.jmh.annotations._
import java.io.File
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS


@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class BenchmarkScala {

    @Benchmark def one(): Unit = {
        Thread.sleep(10)
    }

    @Benchmark def two(): Unit = {
        Thread.sleep(20)
    }
}