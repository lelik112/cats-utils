// The first seven cases port the owner-supplied tests; subsequent cases are added Cats Effect coverage.
package com.parrot669.catsutils

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Outcome, Ref, Resource}
import cats.syntax.all._

import com.parrot669.catsutils.CircuitBreakers.CircuitBreakerOpen

import io.circe.parser.decode
import io.circe.syntax._
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}
import munit.CatsEffectSuite

class CircuitBreakersTest extends CatsEffectSuite {
  private val boom       = new RuntimeException("boom!")
  private val testConfig = CircuitBreakers.Config(
    default = CircuitBreakers.CircuitBreakerConfig(
      failureRate = 0.0001,
      minThroughput = 1,
      halfOpenDuration = Duration.ofSeconds(5),
      openDuration = Duration.ofSeconds(5)
    ),
    paths = Map.empty
  )

  private def failCalls(fixture: TestCircuitBreakers.Fixture, path: String): IO[Unit] = {
    val guarded = fixture.breakers.guard[String]()
    (guarded(path)(IO.raiseError(boom)).attempt *> fixture.clock.advance(1.second)).replicateA_(3)
  }

  test("passes successful calls") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      val guarded = fixture.breakers.guard[String]()
      for {
        _    <- (guarded("path")(IO.pure("test")) *> fixture.clock.advance(1.second)).replicateA_(3)
        read <- guarded("path")(IO.pure("test"))
      } yield assertEquals(read, "test")
    }
  }

  test("opens on errors") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        _    <- failCalls(fixture, "path")
        read <- fixture.breakers.guard[String]()("path")(IO.pure("test")).attempt
      } yield assertEquals(read, Left(CircuitBreakerOpen))
    }
  }

  test("keeps separate breakers for different paths") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        _    <- failCalls(fixture, "foo")
        read <- fixture.breakers.guard[String]()("bar")(IO.pure("test"))
      } yield assertEquals(read, "test")
    }
  }

  test("passes successful calls (scoped)") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        _    <- fixture.breakers.scoped()("path").use(_ => fixture.clock.advance(1.second)).replicateA_(3)
        read <- fixture.breakers.scoped()("path").use_
      } yield assertEquals(read, ())
    }
  }

  test("opens on errors (scoped)") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        _    <- fixture.breakers
                  .scoped()("path")
                  .use(_ => fixture.clock.advance(1.second) *> IO.raiseError[Unit](boom))
                  .attempt
                  .replicateA_(3)
        read <- fixture.breakers.scoped()("path").use_.attempt
      } yield assertEquals(read, Left(CircuitBreakerOpen))
    }
  }

  test("keeps separate breakers for different paths (scoped)") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        _    <- fixture.breakers
                  .scoped()("foo")
                  .use(_ => fixture.clock.advance(1.second) *> IO.raiseError[Unit](boom))
                  .attempt
                  .replicateA_(3)
        read <- fixture.breakers.scoped()("bar").use_
      } yield assertEquals(read, ())
    }
  }

  test("reloads on config changes") {
    Ref.of[IO, CircuitBreakers.Config](testConfig).flatMap { current =>
      TestCircuitBreakers.make(current.get).use { fixture =>
        val guarded = fixture.breakers.guard[String]()
        for {
          _      <- failCalls(fixture, "path")
          before <- guarded("path")(IO.pure("test")).attempt
          _      <- current.set(testConfig.copy(default = testConfig.default.copy(halfOpenDuration = Duration.ofSeconds(3))))
          after  <- guarded("path")(IO.pure("test"))
        } yield {
          assertEquals(before, Left(CircuitBreakerOpen))
          assertEquals(after, "test")
        }
      }
    }
  }

  test("guardEither preserves typed errors and counts them as failures by default") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      val guarded = fixture.breakers.guardEither[String, Int]()
      for {
        first <- guarded("path")(IO.pure(Left("invalid"))).attempt
        _     <- fixture.clock.advance(1.second)
        _     <- (guarded("path")(IO.pure(Left("invalid"))).attempt *> fixture.clock.advance(1.second)).replicateA_(2)
        read  <- guarded("path")(IO.pure(Right(42))).attempt
      } yield {
        assertEquals(first, Right(Left("invalid")))
        assertEquals(read, Left(CircuitBreakerOpen))
      }
    }
  }

  test("guardEither passes successful Right values") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      val guarded = fixture.breakers.guardEither[String, Int]()
      for {
        _    <- (guarded("path")(IO.pure(Right(42))) *> fixture.clock.advance(1.second)).replicateA_(3)
        read <- guarded("path")(IO.pure(Right(42)))
      } yield assertEquals(read, Right(42))
    }
  }

  test("success predicates can inspect typed business results") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      val guarded = fixture.breakers.guardEither[String, Int](
        isSuccess = {
          case Outcome.Succeeded(value) =>
            value.map {
              case Left("expected") => true
              case Right(_)         => true
              case _                => false
            }
          case _                        => IO.pure(false)
        }
      )
      for {
        expected <- (guarded("path")(IO.pure(Left("expected"))) <* fixture.clock.advance(1.second)).replicateA(3)
        read     <- guarded("path")(IO.pure(Right(42)))
      } yield {
        assertEquals(expected, List.fill(3)(Left("expected")))
        assertEquals(read, Right(42))
      }
    }
  }

  test("onOpen receives the path and skips the guarded effect") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        ran    <- Ref.of[IO, Boolean](false)
        _      <- failCalls(fixture, "path")
        read   <- fixture.breakers.guard[String](onOpen = path => IO.pure(s"fallback:$path"))("path")(
                    ran.set(true).as("unexpected")
                  )
        didRun <- ran.get
      } yield {
        assertEquals(read, "fallback:path")
        assert(!didRun)
      }
    }
  }

  test("guard records cancellation exactly once and cancellation counts as failure") {
    TestControl.executeEmbed {
      TestCircuitBreakers.make(testConfig).use { fixture =>
        for {
          observed <- Ref.of[IO, List[Boolean]](Nil)
          started  <- Deferred[IO, Unit]
          guarded   =
            fixture.breakers
              .guard[String](isSuccess = outcome => observed.update(_ :+ outcome.isCanceled).as(outcome.isSuccess))
          fiber    <- guarded("path")(started.complete(()) *> IO.never[String]).start
          _        <- started.get
          _        <- fiber.cancel
          outcome  <- fiber.join
          seen     <- observed.get
          _        <- fixture.clock.advance(1.second)
          _        <- failCalls(fixture, "path")
          read     <- guarded("path")(IO.pure("test")).attempt
        } yield {
          assert(outcome.isCanceled)
          assertEquals(seen, List(true))
          assertEquals(read, Left(CircuitBreakerOpen))
        }
      }
    }
  }

  test("scoped success is recorded on release after the use action") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        observed <- Ref.of[IO, List[Resource.ExitCase]](Nil)
        resource  = fixture.breakers.scoped(isSuccess = exit => observed.update(_ :+ exit).as(true))("path")
        _        <- resource.use(_ => observed.get.map(seen => assertEquals(seen, Nil)))
        seen     <- observed.get
      } yield assertEquals(seen, List(Resource.ExitCase.Succeeded))
    }
  }

  test("scoped failure observers receive the original raised error") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        observed <- Ref.of[IO, List[Resource.ExitCase]](Nil)
        read     <- fixture.breakers
                      .scoped(isSuccess = exit => observed.update(_ :+ exit).as(false))("path")
                      .use(_ => IO.raiseError[Unit](boom))
                      .attempt
        seen     <- observed.get
      } yield {
        assertEquals(read, Left(boom))
        assertEquals(seen, List(Resource.ExitCase.Errored(boom)))
      }
    }
  }

  test("scoped cancellation records a canceled exit exactly once") {
    TestControl.executeEmbed {
      TestCircuitBreakers.make(testConfig).use { fixture =>
        for {
          observed <- Ref.of[IO, List[Resource.ExitCase]](Nil)
          started  <- Deferred[IO, Unit]
          fiber    <- fixture.breakers
                        .scoped(isSuccess = exit => observed.update(_ :+ exit).as(false))("path")
                        .use(_ => started.complete(()) *> IO.never[Unit])
                        .start
          _        <- started.get
          _        <- fiber.cancel
          outcome  <- fiber.join
          seen     <- observed.get
        } yield {
          assert(outcome.isCanceled)
          assertEquals(seen, List(Resource.ExitCase.Canceled))
        }
      }
    }
  }

  test("scoped observers receive errors from nested resource finalizers") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        observed <- Ref.of[IO, List[Resource.ExitCase]](Nil)
        resource  = fixture.breakers
                      .scoped(isSuccess = exit => observed.update(_ :+ exit).as(false))("path")
                      .flatMap(_ => Resource.make(IO.unit)(_ => IO.raiseError[Unit](boom)))
        read     <- resource.use_.attempt
        seen     <- observed.get
      } yield {
        assertEquals(read, Left(boom))
        assertEquals(seen, List(Resource.ExitCase.Errored(boom)))
      }
    }
  }

  test("guardEither can classify a typed Resource use result after all finalizers") {
    TestCircuitBreakers.make(testConfig).use { fixture =>
      for {
        released <- Ref.of[IO, Boolean](false)
        guarded   = fixture.breakers.guardEither[String, Int](isSuccess =
                      outcome =>
                        released.get.flatMap { wasReleased =>
                          IO(assert(wasReleased)) *> CircuitBreakers.eitherIsSuccess(outcome)
                        }
                    )
        read     <-
          guarded("path")(
            Resource.make(IO.unit)(_ => released.set(true)).use(_ => IO.pure[Either[String, Int]](Left("invalid")))
          )
      } yield assertEquals(read, Left("invalid"))
    }
  }

  test("an open scoped fallback stays cancelable and has no admitted-request finalizer") {
    TestControl.executeEmbed {
      TestCircuitBreakers.make(testConfig).use { fixture =>
        for {
          _               <- failCalls(fixture, "path")
          fallbackStarted <- Deferred[IO, Unit]
          observed        <- Ref.of[IO, Int](0)
          fiber           <- fixture.breakers
                               .scoped(
                                 onOpen = _ => fallbackStarted.complete(()) *> IO.never[Unit],
                                 isSuccess = _ => observed.update(_ + 1).as(true)
                               )("path")
                               .use_
                               .start
          _               <- fallbackStarted.get
          _               <- fiber.cancel
          outcome         <- fiber.join
          seen            <- observed.get
        } yield {
          assert(outcome.isCanceled)
          assertEquals(seen, 0)
        }
      }
    }
  }

  test("path overrides select their own throughput threshold") {
    val config = testConfig.copy(paths = Map("tolerant" -> testConfig.default.copy(minThroughput = 100)))
    TestCircuitBreakers.make(config).use { fixture =>
      for {
        _        <- failCalls(fixture, "tolerant")
        _        <- failCalls(fixture, "regular")
        regular  <- fixture.breakers.guard[String]()("regular")(IO.pure("test")).attempt
        tolerant <- fixture.breakers.guard[String]()("tolerant")(IO.pure("test"))
      } yield {
        assertEquals(regular, Left(CircuitBreakerOpen))
        assertEquals(tolerant, "test")
      }
    }
  }

  test("nonpositive shard counts fail during construction") {
    Resource.make(IO.delay(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))(r => IO.delay(r.close())).use {
      registry =>
        CircuitBreakers.make("test", IO.pure(testConfig), registry, shards = 0).attempt.map { result =>
          assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
        }
    }
  }

  test("config retains duration JSON codecs and PureConfig derivation") {
    assertEquals(decode[CircuitBreakers.Config](testConfig.asJson.noSpaces), Right(testConfig))
    val loaded = _root_.pureconfig.ConfigSource
      .string("""
      default { failure-rate = 0.0001, min-throughput = 1, half-open-duration = 5 seconds, open-duration = 5 seconds }
      paths = {}
    """)
      .load[CircuitBreakers.Config]
    assertEquals(loaded, Right(testConfig))
  }
}
