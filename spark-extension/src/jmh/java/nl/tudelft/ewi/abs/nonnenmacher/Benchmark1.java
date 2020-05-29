package nl.tudelft.ewi.abs.nonnenmacher;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
public class Benchmark1 {

    @Benchmark
    public void one() throws Exception {
        Thread.sleep(10);
    }

    @Benchmark
    public void two() throws Exception {
        Thread.sleep(20);
    }
}