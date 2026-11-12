package com.parrot669.catsutils.cache

import java.time.Duration

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all._

import munit.CatsEffectSuite

class SetCacheWeightTest extends CatsEffectSuite {
  List("write", "access").foreach { mode =>
    def cache(size: Int => Int = identity, maximum: Long = Long.MaxValue): Resource[IO, SetCache[IO, Int, Int, Int]] =
      if (mode == "write")
        SetCache.expireAfterWrite[IO, Int, Int, Int]("weight-review", Duration.ofDays(1), size, maximum, false)
      else
        SetCache.expireAfterAccess[IO, Int, Int, Int]("weight-review", Duration.ofDays(1), None, size, maximum, false)

    test(s"$mode: aggregates above Int.MaxValue return successfully without retention for any maximum") {
      TestControl.executeEmbed {
        List(1L, Long.MaxValue).traverse_ { maximum =>
          List(2, 4).traverse_ { count =>
            cache(_ => 1073741824, maximum).use { cache =>
              val values = (1 to count).map(i => i -> i).toMap
              for {
                calls  <- Ref.of[IO, Int](0)
                first  <- cache.getOrUpdate(0, values.keySet)(_ => calls.update(_ + 1).as(values))
                second <- cache.getOrUpdate(0, values.keySet)(_ => calls.update(_ + 1).as(values))
                loads  <- calls.get
              } yield {
                assertEquals(first, values)
                assertEquals(second, values)
                assertEquals(loads, 2)
              }
            }
          }
        }
      }
    }

    test(s"$mode: Int.MaxValue is representable and each value is weighed once per publication") {
      TestControl.executeEmbed {
        IO(new java.util.concurrent.atomic.AtomicInteger(0)).flatMap { calls =>
          cache { value => calls.incrementAndGet(); value }.use { cache =>
            val values = Map(1 -> (Int.MaxValue - 1), 2 -> 1)
            for {
              first  <- cache.getOrUpdate(0, values.keySet)(_ => IO.pure(values))
              second <- cache.getOrUpdate(0, values.keySet)(_ => IO.raiseError(new AssertionError("lost entry")))
            } yield {
              assertEquals(first, values)
              assertEquals(second, values)
              assertEquals(calls.get(), 2)
            }
          }
        }
      }
    }

    test(s"$mode: overflowing merged put evicts the aggregate and permits later valid values") {
      TestControl.executeEmbed(cache().use { cache =>
        for {
          _        <- cache.put(0, Map(1 -> 1073741824))
          _        <- cache.put(0, Map(2 -> 1073741824))
          missing  <- Ref.of[IO, Set[Int]](Set.empty)
          read     <- cache.getOrUpdate(0, Set(1, 2))(keys => missing.set(keys).as(Map(1 -> 1, 2 -> 2)))
          asked    <- missing.get
          retained <- cache.getOrUpdate(0, Set(1, 2))(_ => IO.raiseError(new AssertionError("lost retry")))
        } yield {
          assertEquals(asked, Set(1, 2))
          assertEquals(read, Map(1 -> 1, 2 -> 2))
          assertEquals(retained, read)
        }
      })
    }

    test(s"$mode: negative member weights cannot be hidden by positive weights and preserve previous state") {
      TestControl.executeEmbed(cache().use { cache =>
        for {
          _      <- cache.put(0, Map(1 -> 5))
          failed <- cache.put(0, Map(2 -> -1)).attempt
          read   <- cache.getOrUpdate(0, Set(1))(_ => IO.raiseError(new AssertionError("lost original")))
          retry  <- cache.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(2 -> 3)))
        } yield {
          assert(failed.swap.toOption.exists(_.isInstanceOf[IllegalArgumentException]))
          assertEquals(read, Map(1 -> 5))
          assertEquals(retry, Map(1 -> 5, 2 -> 3))
        }
      })
    }

    test(s"$mode: all weights remain validated even after the aggregate exceeds Int.MaxValue") {
      TestControl.executeEmbed {
        IO(new java.util.concurrent.atomic.AtomicInteger(0)).flatMap { calls =>
          cache { value =>
            val invocation = calls.incrementAndGet()
            if (invocation <= 2) Int.MaxValue else if (invocation == 3) -1 else value
          }.use { cache =>
            val values = Map(1 -> 1, 2 -> 2, 3 -> 3)
            for {
              failed  <- cache.getOrUpdate(0, values.keySet)(_ => IO.pure(values)).attempt
              checked <- IO(calls.get())
              retry   <- cache.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 1)))
            } yield {
              assert(failed.swap.toOption.exists(_.isInstanceOf[IllegalArgumentException]))
              assertEquals(checked, 3)
              assertEquals(retry, Map(1 -> 1))
            }
          }
        }
      }
    }

    test(s"$mode: a throwing member weigher preserves valid data and releases permits") {
      TestControl.executeEmbed {
        val boom = new RuntimeException("weight failed")
        cache(value => if (value == 99) throw boom else value).use { cache =>
          for {
            _      <- cache.put(0, Map(1 -> 1))
            failed <- cache.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(2 -> 99))).attempt
            read   <- cache.getOrUpdate(0, Set(1))(_ => IO.raiseError(new AssertionError("lost original")))
            retry  <- cache.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(2 -> 2)))
          } yield {
            assertEquals(failed, Left(boom))
            assertEquals(read, Map(1 -> 1))
            assertEquals(retry, Map(1 -> 1, 2 -> 2))
          }
        }
      }
    }

    test(s"$mode: stale oversized completion cannot invalidate a replacement") {
      TestControl.executeEmbed(cache().use { cache =>
        for {
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          old     <- cache
                       .getOrUpdate(0, Set(1, 2)) { _ =>
                         started.complete(()) *> release.get.as(Map(1 -> Int.MaxValue, 2 -> Int.MaxValue))
                       }
                       .start
          _       <- started.get *> cache.invalidate(0)
          _       <- cache.put(0, Map(1 -> 1, 2 -> 2))
          _       <- release.complete(()) *> old.joinWithNever
          read    <- cache.getOrUpdate(0, Set(1, 2))(_ => IO.raiseError(new AssertionError("replacement invalidated")))
        } yield assertEquals(read, Map(1 -> 1, 2 -> 2))
      })
    }
  }
}
