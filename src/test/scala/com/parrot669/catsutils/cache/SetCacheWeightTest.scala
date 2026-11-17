package com.parrot669.catsutils.cache

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{Deferred,IO,Ref,Resource}
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

  List(false, true).foreach { access =>
    List("negative", "zero", "empty-group").foreach { kind =>
      test(s"$access: $kind entries share a finite charged budget across groups") {
        TestControl.executeEmbed {
          for {
            gauges  <- Ref.of[IO, Map[String, Double]](Map.empty)
            metrics  = new CacheMetrics[IO] {
                         def path: IO[String] = IO.pure("")
                         def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
                         def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] =
                           gauges.update(_.updated(name, value))
                       }
            resource = if (access)
                         SetCache.expireAfterAccess[IO, Int, Int, Int](
                           "budget",
                           Duration.ofDays(1),
                           None,
                           (_: Int) => 0,
                           3L,
                           kind != "empty-group",
                           metrics
                         )
                       else
                         SetCache.expireAfterWrite[IO, Int, Int, Int](
                           "budget",
                           Duration.ofDays(1),
                           (_: Int) => 0,
                           3L,
                           kind != "empty-group",
                           metrics
                         )
            _       <- resource.use { c =>
                         for {
                           _        <- (1 to 20).toList.traverse_(g =>
                                         c.getOrUpdate(g, Set(1))(_ => IO.pure(if (kind == "zero") Map(1 -> 0) else Map.empty))
                                       )
                           _        <- IO.sleep(31.seconds)
                           recorded <- gauges.get
                         } yield {
                           assert(recorded("cache_weight") > 0.0)
                           assert(recorded("cache_weight") <= 3.0)
                           assert(recorded("cache_size") <= recorded("cache_weight"))
                           assertEquals(recorded("cache_max_weight"), 3.0)
                         }
                       }
          } yield ()
        }
      }
    }

    List(0L, 1L, 3L).foreach { maximum =>
      test(s"$access: mixed negative and zero-sized values charge slots; budget=$maximum; whole group only") {
        val resource =
          if (access)
            SetCache.expireAfterAccess[IO, Int, Int, Int](
              "slots",
              Duration.ofDays(1),
              None,
              (_: Int) => 0,
              maximum,
              true
            )
          else SetCache.expireAfterWrite[IO, Int, Int, Int]("slots", Duration.ofDays(1), (_: Int) => 0, maximum, true)
        TestControl.executeEmbed(resource.use { c =>
          for {
            _       <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(1 -> 0)))
            asked   <- Ref.of[IO, Set[Int]](Set.empty)
            read    <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 1, 2 -> 2)))
            missing <- asked.get
          } yield {
            assertEquals(missing, if (maximum < 2) Set(1, 2) else Set.empty[Int])
            assertEquals(read, if (maximum < 2) Map(1 -> 1, 2 -> 2) else Map(1 -> 0))
          }
        })
      }
    }

    test(s"$access: overwriting a cell does not double its charged weight") {
      val resource =
        if (access)
          SetCache.expireAfterAccess[IO, Int, Int, Int]("overwrite", Duration.ofDays(1), None, 2L, true)
        else SetCache.expireAfterWrite[IO, Int, Int, Int]("overwrite", Duration.ofDays(1), 2L, true)
      TestControl.executeEmbed(resource.use { c =>
        for {
          _    <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map.empty))
          _    <- c.put(0, Map(1 -> 1)) *> c.put(0, Map(1 -> 10))
          read <- c.getOrUpdate(0, Set(1, 2))(_ => IO.raiseError(new AssertionError("overcharged overwrite")))
        } yield assertEquals(read, Map(1 -> 10))
      })
    }

    test(s"$access: capacity eviction during an old fill prevents stale publication") {
      val resource =
        if (access)
          SetCache.expireAfterAccess[IO, Int, Int, Int]("eviction", Duration.ofDays(1), None, 1L, true)
        else SetCache.expireAfterWrite[IO, Int, Int, Int]("eviction", Duration.ofDays(1), 1L, true)
      TestControl.executeEmbed(resource.use { c =>
        for {
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          old     <- c.getOrUpdate(0, Set(1))(_ => started.complete(()) *> release.get.as(Map(1 -> 1))).start
          _       <- started.get
          // Fill enough other groups to displace the old generation; no particular new victim is assumed.
          _       <- (1 to 20).toList.traverse_(g => c.put(g, Map(1 -> g)))
          _       <- c.put(0, Map(1 -> 100))
          _       <- release.complete(()) *> old.joinWithNever
          read    <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 200)))
        } yield assert(read == Map(1 -> 100) || read == Map(1 -> 200))
      })
    }
  }
  List("invalidate", "capacity").foreach { retirement =>
    List("success", "error", "oversize").foreach { completion =>
      test(s"write: $retirement replacement survives old $completion after a long fill") {
        val boom     = new IllegalStateException("old loader")
        val stale    = if (completion == "oversize") Map(1 -> 99, 2 -> 99) else Map(1 -> 1)
        val resource = SetCache.expireAfterWrite[IO, Int, Int, Int](
          "retirement",
          Duration.ofMinutes(15),
          (v: Int) => if (v == 99) Int.MaxValue else 1,
          1L,
          true
        )
        TestControl.executeEmbed(resource.use { c =>
          for {
            entered     <- Deferred[IO, Unit]
            release     <- Deferred[IO, Unit]
            old         <- c.getOrUpdate(0, Set(1, 2)) { _ =>
                             entered.complete(()) *> release.get *>
                               (if (completion == "error") IO.raiseError[Map[Int, Int]](boom) else IO.pure(stale))
                           }.attempt
                             .start
            _           <- entered.get *> IO.sleep(16.minutes)
            _           <- if (retirement == "invalidate") c.invalidate(0)
                           else (1 to 20).toList.traverse_(g => c.put(g, Map(1 -> g)))
            // Completion before release proves a new generation, rather than waiting on the old permit.
            _           <- c.put(0, Map(1 -> 100)).timeout(1.second)
            replacement <- c.getOrUpdate(0, Set(1))(_ => IO.raiseError(new AssertionError("replacement absent")))
            _           <- release.complete(())
            result      <- old.joinWithNever
            retained    <- c.getOrUpdate(0, Set(1))(_ => IO.raiseError(new AssertionError("replacement removed")))
          } yield {
            if (completion == "error") assertEquals(result, Left(boom))
            else assertEquals(result, Right(stale))
            assertEquals(replacement, Map(1 -> 100)); assertEquals(retained, replacement)
          }
        })
      }
    }
  }

}
