package io.github.lelik112.catsutils

import cats.effect.{Deferred, IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all._
import io.github.lelik112.catsutils.cache.SetCache
import io.github.lelik112.catsutils.cache.ranged.{RangeCache, RangeMap}
import io.github.lelik112.catsutils.domain.TimeRange
import java.time.{Duration, Instant}
import munit.CatsEffectSuite

class IndependentReviewTest extends CatsEffectSuite {
  def instant(n: Int): Instant = Instant.EPOCH.plusSeconds(n.toLong)
  def range(a: Int, b: Int): TimeRange = TimeRange.make(instant(a), instant(b)).toOption.get

  test("merged and split gaps cover all values in both directions") {
    TestControl.executeEmbed {
      (for {
        reversed <- List(false, true); a <- (1 to 8).toList; b <- ((a + 1) to 9).toList; split <- (1 to 5).toList
      } yield (reversed, a, b, split)).traverse_ { case (reversed, a, b, split) =>
        val order = if (reversed) Ordering[Instant].reverse else Ordering[Instant]
        val config = RangeCache.Config
          .expireAfterWrite(Duration.ofDays(1))
          .withIgnoreRanges(Duration.ofSeconds(10))
          .withMaxGap(Duration.ofSeconds(split.toLong))
        RangeCache.make[Int, Int]("review", config)(order).use { cache =>
          for {
            _ <- cache.put(0, range(a, b), (a until b).map(i => instant(i) -> -i))
            result <- cache.getOrUpdate(0, range(0, 10))(gap =>
              IO.pure((0 until 10).filter(i => gap.includes(instant(i))).map(i => instant(i) -> i))
            )
            cached <- cache.getOrUpdate(0, range(0, 10))(_ => IO.raiseError(new RuntimeException("unexpected gap")))
          } yield {
            val expected = if (reversed) (0 until 10).reverse.toList else (0 until 10).toList
            assertEquals(result, expected, s"reversed=$reversed cached=[$a,$b) split=$split")
            assertEquals(cached, expected)
          }
        }
      }
    }
  }

  test("SetCache weight must not wrap to zero and retain overweight entries") {
    TestControl.executeEmbed {
      SetCache
        .expireAfterWrite[Int, Int, Int]("review-overflow", Duration.ofDays(1), (_: Int) => 1073741824, 1L, false)
        .use { cache =>
          for {
            calls <- Ref.of[IO, Int](0)
            values = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
            _ <- cache.getOrUpdate(0, values.keySet)(_ => calls.update(_ + 1).as(values))
            _ <- cache.getOrUpdate(0, values.keySet)(_ => calls.update(_ + 1).as(values))
            count <- calls.get
          } yield assertEquals(count, 2, "4 GiB must not fit a weight limit of 1")
        }
    }
  }

  test("RangeMap random operations match a discrete interval model") {
    IO {
      for (reversed <- List(false, true); seed <- 0 until 20) {
        implicit val order: Ordering[Instant] =
          if (reversed) Ordering.fromLessThan[Instant](_.isAfter(_)) else Ordering.fromLessThan[Instant](_.isBefore(_))
        val random = new scala.util.Random(seed)
        var actual = RangeMap.empty[Int]
        var covered = Set.empty[Int]
        var stored = Map.empty[Int, Int]
        for (step <- 0 until 200) {
          val a = random.nextInt(30); val b = a + 1 + random.nextInt(31 - a)
          if (random.nextInt(4) == 0) {
            actual = actual.remove(instant(a)); covered = covered.filter(_ < a); stored = stored.filter(_._1 < a)
          } else {
            val data = (a until b).filter(_ => random.nextBoolean()).map(i => instant(i) -> (step * 100 + i))
            actual = actual.add(range(a, b), data)
            covered = covered ++ (a until b);
            stored = stored.filterNot(x => x._1 >= a && x._1 < b) ++ data.map { case (i, v) =>
              i.getEpochSecond.toInt -> v
            }
          }
          val got = actual.values(range(0, 32)).values.flatten.toList
          val expect = stored.toList.sortBy(_._1).map(_._2)
          assertEquals(got, if (reversed) expect.reverse else expect, s"seed=$seed step=$step")
          val gaps = actual.gaps(range(0, 32)).flatMap(g => (0 until 32).filter(i => g.includes(instant(i)))).toSet
          assertEquals(gaps, (0 until 32).toSet -- covered)
          assertEquals(actual.size, stored.size)
        }
      }
    }
  }
  test("a delayed old configuration read must not reset an already consumed new rate bucket") {
    TestControl.executeEmbed {
      val old = RateLimits.Config(RateLimits.RateLimitsConfig(1, Duration.ofHours(1)), Map.empty)
      val fresh = old.copy(default = old.default.copy(per = Duration.ofHours(2)))
      for {
        config <- Ref.of[IO, RateLimits.Config](old)
        blockNext <- Ref.of[IO, Boolean](false)
        captured <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        configRead = config.get.flatMap(snapshot =>
          blockNext
            .getAndSet(false)
            .flatMap(block => if (block) captured.complete(()) *> release.get.as(snapshot) else IO.pure(snapshot))
        )
        _ <- RateLimits.make(configRead).use { limits =>
          for {
            initial <- limits("user", "path")
            _ <- blockNext.set(true)
            pending <- limits("user", "path").start
            _ <- captured.get
            _ <- config.set(fresh)
            newAllowed <- limits("user", "path")
            exhausted <- limits("user", "path")
            _ <- release.complete(())
            delayed <- pending.joinWithNever
          } yield {
            assert(initial)
            assert(newAllowed)
            assert(!exhausted)
            assert(!delayed, "stale configuration read recreated the exhausted new bucket")
          }
        }
      } yield ()
    }
  }

  test("a delayed old configuration read must not reset an already consumed new historical bucket") {
    TestControl.executeEmbed {
      val old = HistoricalLimits.Config(
        HistoricalLimits.LegacyLimitsConfig(Duration.ofSeconds(1), 1, Duration.ofHours(1)),
        Map.empty
      )
      val fresh = old.copy(default = old.default.copy(per = Duration.ofHours(2)))
      for {
        config <- Ref.of[IO, HistoricalLimits.Config](old)
        blockNext <- Ref.of[IO, Boolean](false)
        captured <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        configRead = config.get.flatMap(snapshot =>
          blockNext
            .getAndSet(false)
            .flatMap(block => if (block) captured.complete(()) *> release.get.as(snapshot) else IO.pure(snapshot))
        )
        _ <- HistoricalLimits.make(configRead).use { limits =>
          val take = limits("user", "path", Instant.EPOCH.minusSeconds(10))
          for {
            initial <- take
            _ <- blockNext.set(true)
            pending <- take.start
            _ <- captured.get
            _ <- config.set(fresh)
            newAllowed <- take
            exhausted <- take
            _ <- release.complete(())
            delayed <- pending.joinWithNever
          } yield {
            assert(initial)
            assert(newAllowed)
            assert(!exhausted)
            assert(!delayed, "stale configuration read recreated the exhausted new bucket")
          }
        }
      } yield ()
    }
  }

}
