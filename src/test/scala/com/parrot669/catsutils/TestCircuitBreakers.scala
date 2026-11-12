// Adapted from the owner-supplied test fixture; registries are now scoped per test.
package com.parrot669.catsutils

import cats.effect.{IO, Resource}

import com.linecorp.armeria.client.circuitbreaker.TestCircuitBreakerBuilder
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}

object TestCircuitBreakers {
  final case class Fixture(breakers: CircuitBreakers[IO], clock: TestCircuitBreakerBuilder.TestClock)

  def make(config: IO[CircuitBreakers.Config]): Resource[IO, Fixture] = for {
    registry <- Resource.make(IO.delay(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))(r => IO.delay(r.close()))
    clock    <- Resource.eval(TestCircuitBreakerBuilder.make)
    breakers <- Resource.eval(
                  CircuitBreakers.make(
                    name = "test",
                    config = config,
                    registry = registry,
                    builder = clock.builder,
                    shards = 1
                  )
                )
  } yield Fixture(breakers, clock)

  def make(config: CircuitBreakers.Config): Resource[IO, Fixture] = make(IO.pure(config))
}
