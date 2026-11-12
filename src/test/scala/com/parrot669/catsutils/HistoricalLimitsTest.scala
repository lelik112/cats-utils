// Ports the four historical-limit tests added to the standalone source reference; extra cases are marked below.
package com.parrot669.catsutils

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import cats.syntax.all._

import com.parrot669.catsutils.cache.CacheMetrics

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

class HistoricalLimitsTest extends CatsEffectSuite {
  private val rule   = HistoricalLimits.LegacyLimitsConfig(
    olderThan = Duration.ofMinutes(1),
    limit = 2,
    per = Duration.ofSeconds(10)
  )
  private val config = HistoricalLimits.Config(default = rule, paths = Map.empty)

  test("recent requests and the exact threshold do not consume the historical bucket") {
    TestControl.executeEmbed {
      HistoricalLimits.make(IO.pure(config)).use { limits =>
        for {
          now        <- IO.realTimeInstant
          recent     <- limits("user", "path", now).replicateA(3)
          boundary   <- limits("user", "path", now.minusSeconds(60)).replicateA(3)
          historical <- limits("user", "path", now.minusSeconds(61)).replicateA(3)
        } yield {
          assert(recent.forall(identity))
          assert(boundary.forall(identity))
          assertEquals(historical, List(true, true, false))
        }
      }
    }
  }

  test("historical buckets refill and isolate users and paths") {
    TestControl.executeEmbed {
      HistoricalLimits.make(IO.pure(config)).use { limits =>
        for {
          old      <- IO.realTimeInstant.map(_.minusSeconds(61))
          first    <- limits("a", "x", old).replicateA(3)
          user     <- limits("b", "x", old)
          path     <- limits("a", "y", old)
          _        <- IO.sleep(10.seconds)
          refilled <- limits("a", "x", old)
        } yield {
          assertEquals(first, List(true, true, false))
          assert(user)
          assert(path)
          assert(refilled)
        }
      }
    }
  }

  test("changed configuration invalidates the old bucket") {
    TestControl.executeEmbed {
      Ref.of[IO, HistoricalLimits.Config](config).flatMap { current =>
        HistoricalLimits.make(current.get).use { limits =>
          for {
            old    <- IO.realTimeInstant.map(_.minusSeconds(61))
            before <- limits("a", "x", old).replicateA(3)
            _      <- current.set(config.copy(default = rule.copy(limit = 1)))
            after  <- limits("a", "x", old).replicateA(2)
          } yield {
            assertEquals(before, List(true, true, false))
            assertEquals(after, List(true, false))
          }
        }
      }
    }
  }

  test("path overrides choose their own limits") {
    TestControl.executeEmbed {
      HistoricalLimits.make(IO.pure(config.copy(paths = Map("special" -> rule.copy(limit = 1))))).use { limits =>
        for {
          old     <- IO.realTimeInstant.map(_.minusSeconds(61))
          regular <- limits("a", "regular", old).replicateA(2)
          special <- limits("a", "special", old).replicateA(2)
        } yield {
          assertEquals(regular, List(true, true))
          assertEquals(special, List(true, false))
        }
      }
    }
  }

  // Added Cats Effect coverage.
  test("a recent request becomes historical as real time advances") {
    TestControl.executeEmbed {
      HistoricalLimits.make(IO.pure(config)).use { limits =>
        for {
          start  <- IO.realTimeInstant
          before <- limits("a", "x", start).replicateA(3)
          _      <- IO.sleep(61.seconds)
          after  <- limits("a", "x", start).replicateA(3)
        } yield {
          assertEquals(before, List(true, true, true))
          assertEquals(after, List(true, true, false))
        }
      }
    }
  }

  test("config retains duration JSON codecs and PureConfig derivation") {
    assertEquals(decode[HistoricalLimits.Config](config.asJson.noSpaces), Right(config))
    val loaded = _root_.pureconfig.ConfigSource
      .string("""
      default { older-than = 1 minute, limit = 2, per = 10 seconds }
      paths = {}
    """)
      .load[HistoricalLimits.Config]
    assertEquals(loaded, Right(config))
  }

  test("optional metrics retain the legacy cache name and recent requests bypass that cache") {
    TestControl.executeEmbed {
      for {
        observations <- Ref.of[IO, Vector[(String, Map[String, String], Double)]](Vector.empty)
        metrics       = new CacheMetrics[IO] {
                          def path: IO[String] = IO.pure("")
                          def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
                          def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] =
                            observations.update(_ :+ ((name, tags, value)))
                        }
        _            <- HistoricalLimits.make(IO.pure(config), metrics).use { limits =>
                          for {
                            now <- IO.realTimeInstant
                            _   <- limits("recent", "x", now).replicateA_(3)
                            _   <- limits("old", "x", now.minusSeconds(61)).replicateA_(2)
                            _   <- IO.sleep(31.seconds)
                          } yield ()
                        }
        seen         <- observations.get
      } yield {
        val tags = Map("name" -> "Legacy Rate Limits")
        assert(seen.contains(("cache_size", tags, 1.0)))
        assert(seen.contains(("cache_misses_total", tags, 1.0)))
        assert(seen.contains(("cache_hits_total", tags, 1.0)))
      }
    }
  }
}
