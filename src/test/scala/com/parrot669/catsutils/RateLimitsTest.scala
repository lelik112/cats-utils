// The first five cases port the owner-supplied tests; the remaining cases are added Cats Effect coverage.
package com.parrot669.catsutils

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

class RateLimitsTest extends CatsEffectSuite {
  private val testConfig = RateLimits.RateLimitsConfig(limit = 3, per = Duration.ofSeconds(10))
  private val config     = RateLimits.Config(default = testConfig, paths = Map.empty)

  // Synthetic test capacity only; preserve the supplied Boolean assertions through a local adapter.
  private def make(source: IO[RateLimits.Config]): Resource[IO, (String, String) => IO[Boolean]] =
    RateLimits.make(source, maxKeys = 16).map { limits => (id: String, path: String) =>
      limits(id, path).map(_.isAllowed)
    }

  test("limits requests") {
    TestControl.executeEmbed {
      make(IO.pure(config)).use { limits =>
        for {
          allowed <- limits("test", "test").replicateA(3)
          denied  <- limits("test", "test")
        } yield {
          assert(allowed.forall(identity))
          assert(!denied)
        }
      }
    }
  }

  test("refills in specified duration") {
    TestControl.executeEmbed {
      make(IO.pure(config)).use { limits =>
        for {
          _       <- limits("test", "test").replicateA(3)
          _       <- IO.sleep(10.seconds)
          allowed <- limits("test", "test")
        } yield assert(allowed)
      }
    }
  }

  test("counts users separately") {
    TestControl.executeEmbed {
      make(IO.pure(config)).use { limits =>
        for {
          _       <- limits("test", "test").replicateA(3)
          allowed <- limits("test2", "test")
        } yield assert(allowed)
      }
    }
  }

  test("counts paths separately") {
    TestControl.executeEmbed {
      make(IO.pure(config)).use { limits =>
        for {
          _       <- limits("test", "test").replicateA(3)
          allowed <- limits("test", "test2")
        } yield assert(allowed)
      }
    }
  }

  test("check heavy limits for heavy users") {
    val regularUser = "RegularUser"
    val heavyUser   = "heavy-user"
    val path        = "/api/v1/items"
    val pathConfig  = testConfig.copy(users = Map(heavyUser -> testConfig.copy(limit = 2, per = Duration.ofSeconds(1))))
    TestControl.executeEmbed {
      make(IO.pure(config.copy(paths = Map(path -> pathConfig)))).use { limits =>
        for {
          regularAllowed <- limits(regularUser, path).replicateA(5).map(_.count(identity))
          heavyAllowed   <- limits(heavyUser, path).replicateA(5).map(_.count(identity))
        } yield {
          assertEquals(regularAllowed, 3)
          assertEquals(heavyAllowed, 2)
        }
      }
    }
  }

  test("changed effective configuration replaces an exhausted bucket") {
    TestControl.executeEmbed {
      Ref.of[IO, RateLimits.Config](config).flatMap { current =>
        make(current.get).use { limits =>
          for {
            before <- limits("a", "x").replicateA(4)
            _      <- current.set(config.copy(default = testConfig.copy(limit = 1)))
            after  <- limits("a", "x").replicateA(2)
          } yield {
            assertEquals(before, List(true, true, true, false))
            assertEquals(after, List(true, false))
          }
        }
      }
    }
  }

  test("concurrent calls cannot spend a token twice") {
    TestControl.executeEmbed {
      make(IO.pure(config)).use { limits =>
        List.fill(100)(limits("a", "x")).parSequence.map(results => assertEquals(results.count(identity), 3))
      }
    }
  }

  test("denied attempts retain partial refill credit and refill is capped at capacity") {
    TestControl.executeEmbed {
      for {
        bucket  <- RateLimits.TokenBucket.make[IO](3, Duration.ofSeconds(10))
        initial <- bucket.take.replicateA(4)
        _       <- IO.sleep(5.seconds)
        half    <- bucket.take
        _       <- IO.sleep(5.seconds)
        full    <- bucket.take.replicateA(2)
        _       <- IO.sleep(100.seconds)
        capped  <- bucket.take.replicateA(4)
      } yield {
        assertEquals(initial, List(true, true, true, false))
        assert(!half)
        assertEquals(full, List(true, false))
        assertEquals(capped, List(true, true, true, false))
      }
    }
  }

  test("zero refill duration is rejected before a bucket is allocated") {
    RateLimits.TokenBucket.make[IO](1, Duration.ZERO).attempt.map { result =>
      assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
    }
  }

  test("invalid capacity and negative refill duration raise exceptions") {
    for {
      capacity <- RateLimits.TokenBucket.make[IO](0, Duration.ofSeconds(1)).attempt
      interval <- RateLimits.TokenBucket.make[IO](1, Duration.ofSeconds(-1)).attempt
    } yield {
      assert(capacity.left.exists(_.isInstanceOf[IllegalArgumentException]))
      assert(interval.left.exists(_.isInstanceOf[IllegalArgumentException]))
    }
  }

  test("config retains duration JSON codecs and PureConfig derivation") {
    val nested = config.copy(default = testConfig.copy(users = Map("heavy" -> testConfig.copy(limit = 2))))
    assertEquals(decode[RateLimits.Config](nested.asJson.noSpaces), Right(nested))
    val loaded = _root_.pureconfig.ConfigSource
      .string("""
      default {
        limit = 3
        per = 10 seconds
        users { heavy { limit = 2, per = 10 seconds } }
      }
      paths = {}
    """)
      .load[RateLimits.Config]
    assertEquals(loaded, Right(nested))
  }

  test("resource release clears storage and a retained handle rejects further calls") {
    for {
      allocated        <- RateLimits.make(IO.pure(config), maxKeys = 1).allocated
      (limits, release) = allocated
      before           <- limits("a", "x")
      _                <- release
      after            <- limits("a", "x").attempt
      stillClosed      <- limits("new", "x").attempt
    } yield {
      assertEquals(before, RateLimits.Allowed)
      assert(after.left.exists(_.isInstanceOf[IllegalStateException]))
      assert(stillClosed.left.exists(_.isInstanceOf[IllegalStateException]))
    }
  }
}
