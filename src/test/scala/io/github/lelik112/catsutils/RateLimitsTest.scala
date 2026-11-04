// The first five cases port the owner-supplied tests; the remaining cases are added Cats Effect coverage.
package io.github.lelik112.catsutils

import java.time.Duration

import scala.concurrent.duration._

import io.github.lelik112.catsutils.cache.CacheMetrics

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all._
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

class RateLimitsTest extends CatsEffectSuite {
  private val testConfig = RateLimits.RateLimitsConfig(limit = 3, per = Duration.ofSeconds(10))
  private val config = RateLimits.Config(default = testConfig, paths = Map.empty)

  test("limits requests") {
    TestControl.executeEmbed {
      RateLimits.make(IO.pure(config)).use { limits =>
        for {
          allowed <- limits("test", "test").replicateA(3)
          denied <- limits("test", "test")
        } yield {
          assert(allowed.forall(identity))
          assert(!denied)
        }
      }
    }
  }

  test("refills in specified duration") {
    TestControl.executeEmbed {
      RateLimits.make(IO.pure(config)).use { limits =>
        for {
          _ <- limits("test", "test").replicateA(3)
          _ <- IO.sleep(10.seconds)
          allowed <- limits("test", "test")
        } yield assert(allowed)
      }
    }
  }

  test("counts users separately") {
    TestControl.executeEmbed {
      RateLimits.make(IO.pure(config)).use { limits =>
        for {
          _ <- limits("test", "test").replicateA(3)
          allowed <- limits("test2", "test")
        } yield assert(allowed)
      }
    }
  }

  test("counts paths separately") {
    TestControl.executeEmbed {
      RateLimits.make(IO.pure(config)).use { limits =>
        for {
          _ <- limits("test", "test").replicateA(3)
          allowed <- limits("test", "test2")
        } yield assert(allowed)
      }
    }
  }

  test("check heavy limits for heavy users") {
    val regularUser = "RegularUser"
    val heavyUser = "heavy-user"
    val path = "/api/v1/items"
    val pathConfig = testConfig.copy(users = Map(heavyUser -> testConfig.copy(limit = 2, per = Duration.ofSeconds(1))))
    TestControl.executeEmbed {
      RateLimits.make(IO.pure(config.copy(paths = Map(path -> pathConfig)))).use { limits =>
        for {
          regularAllowed <- limits(regularUser, path).replicateA(5).map(_.count(identity))
          heavyAllowed <- limits(heavyUser, path).replicateA(5).map(_.count(identity))
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
        RateLimits.make(current.get).use { limits =>
          for {
            before <- limits("a", "x").replicateA(4)
            _ <- current.set(config.copy(default = testConfig.copy(limit = 1)))
            after <- limits("a", "x").replicateA(2)
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
      RateLimits.make(IO.pure(config)).use { limits =>
        List.fill(100)(limits("a", "x")).parSequence.map(results => assertEquals(results.count(identity), 3))
      }
    }
  }

  test("denied attempts retain partial refill credit and refill is capped at capacity") {
    TestControl.executeEmbed {
      for {
        bucket <- RateLimits.TokenBucket.make(3, Duration.ofSeconds(10))
        initial <- bucket.take.replicateA(4)
        _ <- IO.sleep(5.seconds)
        half <- bucket.take
        _ <- IO.sleep(5.seconds)
        full <- bucket.take.replicateA(2)
        _ <- IO.sleep(100.seconds)
        capped <- bucket.take.replicateA(4)
      } yield {
        assertEquals(initial, List(true, true, true, false))
        assert(!half)
        assertEquals(full, List(true, false))
        assertEquals(capped, List(true, true, true, false))
      }
    }
  }

  test("zero refill duration retains the source behavior") {
    TestControl.executeEmbed {
      for {
        bucket <- RateLimits.TokenBucket.make(1, Duration.ZERO)
        initial <- bucket.take.replicateA(2)
        _ <- IO.sleep(1.nanosecond)
        replenished <- bucket.take
      } yield {
        assertEquals(initial, List(true, false))
        assert(replenished)
      }
    }
  }

  test("invalid capacity and negative refill duration raise exceptions") {
    for {
      capacity <- RateLimits.TokenBucket.make(0, Duration.ofSeconds(1)).attempt
      interval <- RateLimits.TokenBucket.make(1, Duration.ofSeconds(-1)).attempt
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

  test("optional metrics record the named bucket cache and stop after resource release") {
    TestControl.executeEmbed {
      for {
        observations <- Ref.of[IO, Vector[(String, Map[String, String], Double)]](Vector.empty)
        metrics = new CacheMetrics {
          def path: IO[String] = IO.pure("")
          def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
          def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] =
            observations.update(_ :+ ((name, tags, value)))
        }
        _ <- RateLimits.make(IO.pure(config), metrics).use { limits =>
          limits("a", "x").replicateA_(2) *> IO.sleep(31.seconds)
        }
        atRelease <- observations.get
        _ <- IO.sleep(31.seconds)
        afterRelease <- observations.get
      } yield {
        val tags = Map("name" -> "Rate Limits")
        assert(atRelease.contains(("cache_size", tags, 1.0)))
        assert(atRelease.contains(("cache_misses_total", tags, 1.0)))
        assert(atRelease.contains(("cache_hits_total", tags, 1.0)))
        assertEquals(afterRelease, atRelease)
      }
    }
  }
}
