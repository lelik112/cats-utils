// Adapted from the owner-supplied test helper; Armeria's package is required for ticker access.
package com.linecorp.armeria.client.circuitbreaker

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.FiniteDuration

import cats.effect.IO
import com.linecorp.armeria.common.util.Ticker

object TestCircuitBreakerBuilder {
  final class TestClock private[TestCircuitBreakerBuilder] (nanos: AtomicLong) {
    private val ticker: Ticker = new Ticker {
      override def read(): Long = nanos.get()
    }

    val builder: String => CircuitBreakerBuilder = name => CircuitBreaker.builder(name).ticker(ticker)

    // A synchronous Armeria ticker needs a synchronous clock. Advance it explicitly instead of running IO unsafely.
    def advance(duration: FiniteDuration): IO[Unit] = IO.delay {
      nanos.addAndGet(duration.toNanos)
      ()
    }
  }

  def make: IO[TestClock] = IO.delay(new TestClock(new AtomicLong(0L)))
}
