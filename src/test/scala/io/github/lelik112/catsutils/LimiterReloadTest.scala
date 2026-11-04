package io.github.lelik112.catsutils

import java.time.{Duration, Instant}
import scala.concurrent.duration._

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all._
import munit.CatsEffectSuite

/** Additional public-API regressions for the inherited configuration-reload race.
  * The independent review's exact reproducer is retained separately, without modification.
  */
object LimiterReloadTest {
  private type Call = (String, String, Instant) => IO[Boolean]

  final private case class Rule(
      limit: Int = 1,
      per: Duration = Duration.ofHours(1),
      olderThan: Duration = Duration.ofSeconds(1)
  ) {
    def rate: RateLimits.RateLimitsConfig = RateLimits.RateLimitsConfig(limit, per)
    def historical: HistoricalLimits.LegacyLimitsConfig = HistoricalLimits.LegacyLimitsConfig(olderThan, limit, per)
  }

  final private case class Settings(default: Rule = Rule(), paths: Map[String, Rule] = Map.empty) {
    def rate: RateLimits.Config = RateLimits.Config(default.rate, paths.map { case (key, value) => key -> value.rate })
    def historical: HistoricalLimits.Config =
      HistoricalLimits.Config(default.historical, paths.map { case (key, value) => key -> value.historical })
  }

  final private case class LimiterFixture(name: String, make: IO[Settings] => Resource[IO, Call])

  private val fixtures = List(
    LimiterFixture(
      "RateLimits",
      source =>
        RateLimits.make(source.map(_.rate)).map { limits => (id: String, path: String, start: Instant) =>
          limits(id, path)
        }
    ),
    LimiterFixture(
      "HistoricalLimits",
      source =>
        HistoricalLimits.make(source.map(_.historical)).map { limits => (id: String, path: String, start: Instant) =>
          limits(id, path, start)
        }
    )
  )

  private val initial = Settings()
  private val changed = initial.copy(default = initial.default.copy(per = Duration.ofHours(2)))
  private val oldTime = Instant.EPOCH.minusSeconds(1000)
  private val readFailure = new RuntimeException("configuration read failed")

  final private case class Reads(
      current: Ref[IO, Settings],
      pauseNext: Ref[IO, Boolean],
      captured: Deferred[IO, Unit],
      release: Deferred[IO, Unit],
      failNext: Ref[IO, Boolean],
      neverNext: Ref[IO, Boolean],
      neverStarted: Deferred[IO, Unit]
  ) {
    val read: IO[Settings] = failNext.getAndSet(false).flatMap {
      case true => IO.raiseError(readFailure)
      case false =>
        neverNext.getAndSet(false).flatMap {
          case true => neverStarted.complete(()) *> IO.never[Settings]
          case false =>
            current.get.flatMap { snapshot =>
              pauseNext.getAndSet(false).flatMap {
                case true  => captured.complete(()) *> release.get.as(snapshot)
                case false => IO.pure(snapshot)
              }
            }
        }
    }
  }

  private def reads(settings: Settings = initial): IO[Reads] = for {
    current <- Ref.of[IO, Settings](settings)
    pauseNext <- Ref.of[IO, Boolean](false)
    captured <- Deferred[IO, Unit]
    release <- Deferred[IO, Unit]
    failNext <- Ref.of[IO, Boolean](false)
    neverNext <- Ref.of[IO, Boolean](false)
    neverStarted <- Deferred[IO, Unit]
  } yield Reads(current, pauseNext, captured, release, failNext, neverNext, neverStarted)

  private def take(call: Call): IO[Boolean] = call("user", "path", oldTime)
  private def check(program: IO[Unit]): IO[Unit] = TestControl.executeEmbed(program.timeout(5.seconds))

}

class LimiterReloadTest extends CatsEffectSuite {
  import LimiterReloadTest._

  fixtures.foreach { fixture =>
    test(s"${fixture.name}: an unchanged newer snapshot still outranks an older different read") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            first <- take(call)
            _ <- control.current.set(changed) *> control.pauseNext.set(true)
            pending <- take(call).start
            _ <- control.captured.get *> control.current.set(initial)
            newer <- take(call)
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            after <- take(call)
          } yield {
            assert(first)
            assert(!newer)
            assert(!delayed)
            assert(!after)
          }
        }
      })
    }

    test(s"${fixture.name}: an accepted A-B-A change cannot be undone by a delayed B read") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            a <- take(call)
            _ <- control.current.set(changed) *> control.pauseNext.set(true)
            pending <- take(call).start
            _ <- control.captured.get
            b <- take(call)
            bExhausted <- take(call)
            _ <- control.current.set(initial)
            nextA <- take(call)
            aExhausted <- take(call)
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            after <- take(call)
          } yield {
            assert(a && b && nextA)
            assert(!bExhausted && !aExhausted && !delayed && !after)
          }
        }
      })
    }

    test(s"${fixture.name}: a delayed read for an uncached key uses the latest full configuration") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            _ <- call("other", "path", oldTime)
            _ <- control.pauseNext.set(true)
            pending <- take(call).start
            _ <- control.captured.get *> control.current.set(changed.copy(default = changed.default.copy(limit = 2)))
            other <- call("other", "path", oldTime)
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            remaining <- take(call).replicateA(2)
          } yield {
            assert(other && delayed)
            assertEquals(remaining, List(true, false))
          }
        }
      })
    }

    test(s"${fixture.name}: changing one path preserves an unrelated exhausted budget") {
      val before = initial.copy(paths = Map("special" -> initial.default))
      val after = before.copy(paths = Map("special" -> changed.default.copy(limit = 2)))
      check(reads(before).flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            regular <- call("user", "regular", oldTime)
            special <- call("user", "special", oldTime)
            _ <- control.current.set(after)
            newSpecial <- call("user", "special", oldTime).replicateA(3)
            sameRegular <- call("user", "regular", oldTime)
          } yield {
            assert(regular && special)
            assertEquals(newSpecial, List(true, true, false))
            assert(!sameRegular)
          }
        }
      })
    }

    test(s"${fixture.name}: concurrent requests share one replacement budget") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            first <- take(call)
            _ <- control.current.set(changed.copy(default = changed.default.copy(limit = 3)))
            results <- List.fill(100)(take(call)).parSequence
            exhausted <- take(call)
          } yield {
            assert(first)
            assertEquals(results.count(identity), 3)
            assert(!exhausted)
          }
        }
      })
    }

    test(s"${fixture.name}: invalid accepted configuration preserves the old exhausted bucket") {
      val invalid = changed.copy(default = changed.default.copy(limit = 0))
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            first <- take(call)
            _ <- control.pauseNext.set(true)
            pending <- take(call).attempt.start
            _ <- control.captured.get *> control.current.set(invalid)
            failed <- take(call).attempt
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            _ <- control.current.set(initial)
            restored <- take(call)
          } yield {
            assert(first)
            assert(failed.left.exists(_.isInstanceOf[IllegalArgumentException]))
            assert(delayed.left.exists(_.isInstanceOf[IllegalArgumentException]))
            assert(!restored)
          }
        }
      })
    }

    test(s"${fixture.name}: a failed later read does not discard an earlier successful observation") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            first <- take(call)
            _ <- control.current.set(changed) *> control.pauseNext.set(true)
            pending <- take(call).start
            _ <- control.captured.get *> control.failNext.set(true)
            failed <- take(call).attempt
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            exhausted <- take(call)
          } yield {
            assert(first && delayed)
            assertEquals(failed, Left(readFailure))
            assert(!exhausted)
          }
        }
      })
    }

    test(s"${fixture.name}: a canceled later read does not block or supersede an earlier successful observation") {
      check(reads().flatMap { control =>
        fixture.make(control.read).use { call =>
          for {
            first <- take(call)
            _ <- control.current.set(changed) *> control.pauseNext.set(true)
            pending <- take(call).start
            _ <- control.captured.get *> control.neverNext.set(true)
            canceled <- take(call).start
            _ <- control.neverStarted.get *> canceled.cancel
            outcome <- canceled.join
            _ <- control.release.complete(())
            delayed <- pending.joinWithNever
            exhausted <- take(call)
          } yield {
            assert(first && delayed)
            assert(outcome.isCanceled)
            assert(!exhausted)
          }
        }
      })
    }
  }

  test("HistoricalLimits: an old threshold cannot bypass a newer exhausted historical budget") {
    val recent = initial.copy(default = initial.default.copy(olderThan = Duration.ofMinutes(1)))
    val historical = changed.copy(default = changed.default.copy(olderThan = Duration.ofSeconds(1)))
    check(reads(recent).flatMap { control =>
      HistoricalLimits.make(control.read.map(_.historical)).use { limits =>
        val take = limits("user", "path", Instant.EPOCH.minusSeconds(10))
        for {
          _ <- control.pauseNext.set(true)
          pending <- take.start
          _ <- control.captured.get *> control.current.set(historical)
          first <- take
          exhausted <- take
          _ <- control.release.complete(())
          delayed <- pending.joinWithNever
        } yield {
          assert(first)
          assert(!exhausted && !delayed)
        }
      }
    })
  }
}
