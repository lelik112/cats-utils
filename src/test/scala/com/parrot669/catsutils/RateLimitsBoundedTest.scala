package com.parrot669.catsutils

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

import cats.Applicative
import cats.effect.testkit.TestControl
import cats.effect.{Clock, Deferred, IO, Ref}
import cats.syntax.all._

import munit.CatsEffectSuite

/** KAN-180 synthetic policies; none of these values is a production default. */
class RateLimitsBoundedTest extends CatsEffectSuite {
  import RateLimits._

  private def config(capacity: Int, per: Duration): Config = Config(RateLimitsConfig(capacity, per), Map.empty)
  private val hour   = Duration.ofHours(1)
  private val minute = Duration.ofMinutes(1)

  test("ten exhausted minute tokens do not reset after 61 idle seconds") {
    TestControl.executeEmbed {
      make(IO.pure(config(10, minute)), maxKeys = 1).use { limits =>
        for {
          initial <- limits("a", "search").replicateA(10)
          empty   <- limits("a", "search")
          _       <- IO.sleep(61.seconds)
          one     <- limits("a", "search")
          rest    <- limits("a", "search").replicateA(9)
        } yield {
          assertEquals(initial, List.fill(10)(Allowed))
          assertEquals(empty, Exhausted(minute))
          assertEquals(one, Allowed)
          assertEquals(rest, List.fill(9)(Exhausted(Duration.ofSeconds(59))))
        }
      }
    }
  }

  test("a new identity retires an exhausted slot only at exact full recovery") {
    TestControl.executeEmbed {
      make(IO.pure(config(10, minute)), maxKeys = 1).use { limits =>
        for {
          _      <- limits("a", "p").replicateA_(10)
          _      <- IO.sleep(599.seconds)
          before <- limits("b", "p")
          _      <- IO.sleep(1.second)
          at     <- limits("b", "p")
          old    <- limits("a", "p")
        } yield {
          assertEquals(before, CapacityRejected)
          assertEquals(at, Allowed)
          assertEquals(old, CapacityRejected)
        }
      }
    }
  }

  test("saturation rejects many new keys without evicting existing debt") {
    make(IO.pure(config(1, hour)), maxKeys = 2).use { limits =>
      for {
        a         <- limits("a", "p")
        b         <- limits("b", "p")
        denied    <- (1 to 200).toList.traverse(i => limits(i.toString, "p"))
        old       <- limits("a", "p")
        otherPath <- limits("a", "new-path")
      } yield {
        assertEquals(a, Allowed)
        assertEquals(b, Allowed)
        assertEquals(denied, List.fill(200)(CapacityRejected))
        assert(old.isInstanceOf[Exhausted])
        assertEquals(otherPath, CapacityRejected)
      }
    }
  }

  test("concurrent identities cannot exceed the key cap") {
    TestControl.executeEmbed {
      make(IO.pure(config(1, hour)), maxKeys = 3).use { limits =>
        (1 to 50).toList.parTraverse(i => limits(i.toString, "p")).map { decisions =>
          assertEquals(decisions.count(_ == Allowed), 3)
          assertEquals(decisions.count(_ == CapacityRejected), 47)
        }
      }
    }
  }

  test("partial credit is retained exactly and Retry-After rounds up, not down") {
    TestControl.executeEmbed {
      make(IO.pure(config(1, Duration.ofNanos(1000000001L))), maxKeys = 1).use { limits =>
        for {
          _      <- limits("a", "p")
          first  <- limits("a", "p")
          _      <- IO.sleep(1.nanosecond)
          second <- limits("a", "p")
          _      <- IO.sleep(999999999.nanoseconds)
          last   <- limits("a", "p")
          _      <- IO.sleep(1.nanosecond)
          ready  <- limits("a", "p")
        } yield {
          assertEquals(first, Exhausted(Duration.ofNanos(1000000001L)))
          assertEquals(first.asInstanceOf[Exhausted].retryAfterSeconds, 2L)
          assertEquals(second, Exhausted(Duration.ofSeconds(1)))
          assertEquals(second.asInstanceOf[Exhausted].retryAfterSeconds, 1L)
          assertEquals(last, Exhausted(Duration.ofNanos(1)))
          assertEquals(last.asInstanceOf[Exhausted].retryAfterSeconds, 1L)
          assertEquals(ready, Allowed)
        }
      }
    }
  }

  test("a recovered cold key can retire while an unrelated debtor survives") {
    TestControl.executeEmbed {
      make(IO.pure(config(2, minute)), maxKeys = 2).use { limits =>
        for {
          _     <- limits("cold", "p")
          _     <- limits("hot", "p").replicateA_(2)
          _     <- IO.sleep(60.seconds)
          fresh <- limits("new", "p")
          hot   <- limits("hot", "p").replicateA(2)
          cold  <- limits("cold", "p")
        } yield {
          assertEquals(fresh, Allowed)
          assertEquals(hot, List(Allowed, Exhausted(minute)))
          assertEquals(cold, CapacityRejected)
        }
      }
    }
  }

  test("effective config reload resets in place but cannot bypass capacity for another key") {
    TestControl.executeEmbed {
      val original = config(1, hour)
      Ref.of[IO, Config](original).flatMap { source =>
        make(source.get, maxKeys = 1).use { limits =>
          for {
            _     <- limits("a", "p")
            _     <- source.set(original.copy(default = original.default.copy(limit = 2)))
            reset <- limits("a", "p").replicateA(3)
            other <- limits("b", "p")
          } yield {
            assertEquals(reset, List(Allowed, Allowed, Exhausted(hour)))
            assertEquals(other, CapacityRejected)
          }
        }
      }
    }
  }

  test("changing another user's override does not reset this key's effective rule") {
    TestControl.executeEmbed {
      val original = config(1, hour)
      Ref.of[IO, Config](original).flatMap { source =>
        make(source.get, maxKeys = 2).use { limits =>
          for {
            _         <- limits("a", "p")
            _         <- source.set(
                           original.copy(default =
                             original.default.copy(
                               users = Map("other" -> RateLimitsConfig(2, hour))
                             )
                           )
                         )
            unchanged <- limits("a", "p")
            other     <- limits("other", "p").replicateA(3)
          } yield {
            assertEquals(unchanged, Exhausted(hour))
            assertEquals(other, List(Allowed, Allowed, Exhausted(hour)))
          }
        }
      }
    }
  }

  test("an invalid full snapshot does not discard debt or free occupied capacity") {
    TestControl.executeEmbed {
      val original = config(1, hour)
      Ref.of[IO, Config](original).flatMap { source =>
        make(source.get, maxKeys = 1).use { limits =>
          for {
            _         <- limits("a", "p")
            _         <- source.set(original.copy(paths = Map("unused" -> RateLimitsConfig(0, hour))))
            invalid   <- limits("a", "p").attempt
            _         <- source.set(original)
            preserved <- limits("a", "p")
            newKey    <- limits("b", "p")
          } yield {
            assert(invalid.left.exists(_.isInstanceOf[IllegalArgumentException]))
            assertEquals(preserved, Exhausted(hour))
            assertEquals(newKey, CapacityRejected)
          }
        }
      }
    }
  }

  test("an invalid newer snapshot suppresses a different delayed rule without resetting debt") {
    TestControl.executeEmbed {
      val original = config(1, hour)
      val invalid  = original.copy(paths = Map("unused" -> RateLimitsConfig(0, hour)))
      for {
        current  <- Ref.of[IO, Config](original)
        pause    <- Ref.of[IO, Boolean](false)
        captured <- Deferred[IO, Unit]
        release  <- Deferred[IO, Unit]
        read      = current.get.flatMap(c =>
                      pause.getAndSet(false).flatMap {
                        case true  => captured.complete(()) *> release.get.as(c)
                        case false => IO.pure(c)
                      }
                    )
        _        <- make(read, maxKeys = 1).use { limits =>
                      for {
                        first     <- limits("a", "p")
                        _         <- current.set(config(2, hour)) *> pause.set(true)
                        pending   <- limits("a", "p").attempt.start
                        _         <- captured.get *> current.set(invalid)
                        failed    <- limits("a", "p").attempt
                        _         <- release.complete(())
                        delayed   <- pending.joinWithNever
                        _         <- current.set(original)
                        preserved <- limits("a", "p")
                        newKey    <- limits("b", "p")
                      } yield {
                        assertEquals(first, Allowed)
                        assert(failed.left.exists(_.isInstanceOf[IllegalArgumentException]))
                        assert(delayed.left.exists(_.isInstanceOf[IllegalArgumentException]))
                        assertEquals(preserved, Exhausted(hour))
                        assertEquals(newKey, CapacityRejected)
                      }
                    }
      } yield ()
    }
  }

  test("all rule and max-key bounds are validated, including nested overrides and overflow") {
    TestControl.executeEmbed {
      val invalid = List(
        config(0, minute),
        config(-1, minute),
        config(1, Duration.ZERO),
        config(1, Duration.ofNanos(-1)),
        config(1, Duration.ofSeconds(Long.MaxValue)),
        config(2, Duration.ofNanos(Long.MaxValue)),
        config(1, minute).copy(default = RateLimitsConfig(1, minute, Map("u" -> RateLimitsConfig(1, Duration.ZERO))))
      )
      for {
        policies <- invalid.traverse(c => make(IO.pure(c), maxKeys = 1).use(_("a", "p")).attempt)
        caps     <- List(0, -1).traverse(n => make(IO.pure(config(1, minute)), n).use(_("a", "p")).attempt)
        exact    <- TokenBucket.make[IO](1, Duration.ofNanos(Long.MaxValue))
        _        <- exact.take
        delayed  <- exact.decision
      } yield {
        assert(policies.forall(_.left.exists(_.isInstanceOf[IllegalArgumentException])))
        assert(caps.forall(_.left.exists(_.isInstanceOf[IllegalArgumentException])))
        assertEquals(delayed, Exhausted(Duration.ofNanos(Long.MaxValue)))
        assertEquals(delayed.asInstanceOf[Exhausted].retryAfterSeconds, 9223372037L)
      }
    }
  }

  test("signed clock origins, backwards observations and very large elapsed durations do not overflow") {
    for {
      time      <- Ref.of[IO, FiniteDuration](-10L.nanoseconds)
      clock      = new Clock[IO] {
                     override val applicative: Applicative[IO]  = IO.asyncForIO
                     override def monotonic: IO[FiniteDuration] = time.get
                     override def realTime: IO[FiniteDuration]  = IO.pure(0.seconds)
                   }
      bucket    <- TokenBucket.make[IO](1, Duration.ofNanos(10))(IO.asyncForIO, clock, implicitly[Ref.Make[IO]])
      _         <- bucket.take
      _         <- time.set(-20L.nanoseconds)
      backwards <- bucket.decision
      _         <- time.set(-5L.nanoseconds)
      half      <- bucket.decision
      _         <- time.set(0.nanoseconds)
      full      <- bucket.decision
      _         <- time.set(FiniteDuration(Long.MaxValue, TimeUnit.NANOSECONDS))
      huge      <- bucket.decision
      again     <- bucket.decision
      _         <- time.set((-Long.MaxValue).nanoseconds)
      boundary  <- TokenBucket.make[IO](1, Duration.ofNanos(10))(IO.asyncForIO, clock, implicitly[Ref.Make[IO]])
      _         <- boundary.take
      _         <- time.set(Long.MaxValue.nanoseconds)
      across    <- boundary.decision
    } yield {
      assertEquals(backwards, Exhausted(Duration.ofNanos(10)))
      assertEquals(half, Exhausted(Duration.ofNanos(5)))
      assertEquals(full, Allowed)
      assertEquals(huge, Allowed)
      assertEquals(again, Exhausted(Duration.ofNanos(10)))
      assertEquals(across, Allowed)
    }
  }

  test("late stale observation uses the newer snapshot and cannot reset its debt") {
    TestControl.executeEmbed {
      val original = config(1, hour)
      for {
        current  <- Ref.of[IO, Config](original)
        pause    <- Ref.of[IO, Boolean](false)
        captured <- Deferred[IO, Unit]
        release  <- Deferred[IO, Unit]
        read      = current.get.flatMap(c =>
                      pause.getAndSet(false).flatMap {
                        case true  => captured.complete(()) *> release.get.as(c)
                        case false => IO.pure(c)
                      }
                    )
        _        <- make(read, maxKeys = 1).use { limits =>
                      for {
                        _      <- limits("a", "p")
                        _      <- pause.set(true)
                        slow   <- limits("a", "p").start
                        _      <- captured.get
                        _      <- current.set(config(2, hour))
                        first  <- limits("a", "p")
                        second <- limits("a", "p")
                        _      <- release.complete(())
                        late   <- slow.joinWithNever
                      } yield {
                        assertEquals(first, Allowed)
                        assertEquals(second, Allowed)
                        assertEquals(late, Exhausted(hour))
                      }
                    }
      } yield ()
    }
  }

  test("cancelling a pending config read neither consumes capacity nor blocks a later request") {
    TestControl.executeEmbed {
      for {
        pause   <- Ref.of[IO, Boolean](true)
        started <- Deferred[IO, Unit]
        read     = pause.getAndSet(false).flatMap {
                     case true  => started.complete(()) *> IO.never[Config]
                     case false => IO.pure(config(1, hour))
                   }
        _       <- make(read, maxKeys = 1).use { limits =>
                     for {
                       pending  <- limits("cancelled", "p").start
                       _        <- started.get
                       _        <- pending.cancel
                       admitted <- limits("a", "p")
                       denied   <- limits("a", "p")
                     } yield {
                       assertEquals(admitted, Allowed)
                       assertEquals(denied, Exhausted(hour))
                     }
                   }
      } yield ()
    }
  }

  test("closing while a config read is pending prevents storage resurrection") {
    TestControl.executeEmbed {
      for {
        entered          <- Deferred[IO, Unit]
        resume           <- Deferred[IO, Unit]
        allocated        <- make(entered.complete(()) *> resume.get.as(config(1, hour)), maxKeys = 1).allocated
        (limits, release) = allocated
        pending          <- limits("a", "p").attempt.start
        _                <- entered.get
        _                <- release
        _                <- resume.complete(())
        result           <- pending.joinWithNever
        after            <- limits("b", "p").attempt
      } yield {
        assert(result.left.exists(_.isInstanceOf[IllegalStateException]))
        assert(after.left.exists(_.isInstanceOf[IllegalStateException]))
      }
    }
  }
}
