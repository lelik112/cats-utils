package com.parrot669.catsutils.cache

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{Deferred,IO,Ref,Resource}
import cats.syntax.all._

import munit.CatsEffectSuite

class SetCacheContractTest extends CatsEffectSuite {
  private val ttl = Duration.ofSeconds(10)
  private def cache(
    access:  Boolean = false,
    empty:   Boolean = true,
    maximum: Long = 100L,
    refresh: Option[Duration] = None
  ): Resource[IO, SetCache[IO, Int, Int, Int]] =
    if (access) SetCache.expireAfterAccess[IO, Int, Int, Int]("contract", ttl, refresh, maximum, empty)
    else SetCache.expireAfterWrite[IO, Int, Int, Int]("contract", ttl, maximum, empty)

  List(false, true).foreach { negative =>
    test(s"write: loader16m retains a full15m completion age; negative=$negative") {
      val resource = SetCache.expireAfterWrite[IO, Int, Int, Int](
        "long-write",
        Duration.ofMinutes(15),
        1536L,
        true
      )
      TestControl.executeEmbed(resource.use { c =>
        for {
          calls         <- Ref.of[IO, Int](0)
          load           = calls.getAndUpdate(_ + 1).flatMap { prior =>
                             val result = if (negative) Map.empty[Int, Int] else Map(1 -> 10)
                             if (prior == 0) IO.sleep(16.minutes).as(result) else IO.pure(result)
                           }
          start         <- IO.monotonic
          first         <- c.getOrUpdate(0, Set(1))(_ => load)
          completed     <- IO.monotonic
          immediate     <- c.getOrUpdate(0, Set(1))(_ => load)
          atCompletion  <- calls.get
          _             <- IO.sleep(14.minutes + 59.seconds)
          before        <- c.getOrUpdate(0, Set(1))(_ => load)
          beforeCount   <- calls.get
          _             <- IO.sleep(1.second)
          boundary      <- c.getOrUpdate(0, Set(1))(_ => load)
          boundaryCount <- calls.get
        } yield {
          assertEquals(completed - start, 16.minutes)
          assertEquals(first, if (negative) Map.empty[Int, Int] else Map(1 -> 10))
          assertEquals(immediate, first); assertEquals(before, first); assertEquals(boundary, first)
          assertEquals((atCompletion, beforeCount, boundaryCount), (1, 1, 2))
        }
      })
    }
  }

  test("write: a waiter after TTL waits for the same loader; other groups remain independent") {
    TestControl.executeEmbed(cache().use { c =>
      for {
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        done    <- Deferred[IO, Unit]
        owner   <- c.getOrUpdate(0, Set(1))(_ => entered.complete(()) *> release.get.as(Map(1 -> 1))).start
        _       <- entered.get *> IO.sleep(11.seconds)
        waiter  <- c.getOrUpdate(0, Set(1))(unexpected).flatTap(_ => done.complete(())).start
        other   <- c.getOrUpdate(1, Set(1))(_ => IO.pure(Map(1 -> 100)))
        _       <- IO.sleep(1.nanosecond)
        pending <- done.tryGet
        _       <- release.complete(())
        first   <- owner.joinWithNever
        second  <- waiter.joinWithNever
      } yield {
        assertEquals(pending, None); assertEquals(other, Map(1 -> 100))
        assertEquals(first, Map(1 -> 1)); assertEquals(second, first)
      }
    })
  }

  test("write: idle expired cells remain charged and are pruned by the next publication") {
    TestControl.executeEmbed {
      for {
        gauges <- Ref.of[IO, Map[String, Double]](Map.empty)
        metrics = new CacheMetrics[IO] {
                    override def path: IO[String] = IO.pure("")
                    override def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
                    override def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] =
                      gauges.update(_.updated(name, value))
                  }
        _      <- SetCache.expireAfterWrite[IO, Int, Int, Int]("idle", ttl, 2L, true, metrics).use { c =>
                    for {
                      _       <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(1 -> 1)))
                      _       <- IO.sleep(31.seconds)
                      idle    <- gauges.get
                      asked   <- Ref.of[IO, Set[Int]](Set.empty)
                      fresh   <- c.getOrUpdate(0, Set(1))(keys => asked.set(keys).as(Map(1 -> 10)))
                      missing <- asked.get
                      _       <- IO.sleep(31.seconds)
                      pruned  <- gauges.get
                    } yield {
                      assertEquals(idle("cache_weight"), 2.0)
                      assertEquals(idle("cache_size"), 1.0)
                      assertEquals(missing, Set(1)); assertEquals(fresh, Map(1 -> 10))
                      assertEquals(pruned("cache_weight"), 1.0)
                      assert(pruned("cache_weight") <= pruned("cache_max_weight"))
                    }
                  }
      } yield ()
    }
  }

  private def unexpected(keys: Set[Int]): IO[Map[Int, Int]] =
    IO.raiseError(new AssertionError(s"unexpected missing keys: $keys"))

  List(false, true).foreach { negative =>
    List(9.seconds, 10.seconds, 11.seconds).foreach { age =>
      test(s"write: adding B does not refresh ${if (negative) "negative" else "positive"} A at $age") {
        TestControl.executeEmbed(cache().use { c =>
          for {
            _       <- c.getOrUpdate(0, Set(1))(_ => IO.pure(if (negative) Map.empty else Map(1 -> 1)))
            _       <- IO.sleep(5.seconds) *> c.getOrUpdate(0, Set(2))(_ => IO.pure(Map(2 -> 2)))
            _       <- IO.sleep(age - 5.seconds)
            asked   <- Ref.of[IO, Set[Int]](Set.empty)
            result  <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10)))
            missing <- asked.get
          } yield {
            assertEquals(missing, if (age < 10.seconds) Set.empty[Int] else Set(1))
            assertEquals(
              result,
              if (age < 10.seconds) if (negative) Map(2 -> 2) else Map(1 -> 1, 2 -> 2)
              else Map(1 -> 10, 2 -> 2)
            )
          }
        })
      }
    }

    test(s"access: touching ${if (negative) "negative" else "positive"} A leaves B's boundary unchanged") {
      TestControl.executeEmbed(cache(access = true).use { c =>
        for {
          _       <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(if (negative) Map(2 -> 2) else Map(1 -> 1, 2 -> 2)))
          _       <- IO.sleep(9.seconds) *> c.getOrUpdate(0, Set(1))(unexpected)
          _       <- IO.sleep(1.second)
          asked   <- Ref.of[IO, Set[Int]](Set.empty)
          read    <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(2 -> 20)))
          missing <- asked.get
          _       <- IO.sleep(10.seconds)
          expired <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 10)))
        } yield {
          assertEquals(missing, Set(2))
          assertEquals(read, if (negative) Map(2 -> 20) else Map(1 -> 1, 2 -> 20))
          assertEquals(expired, Map(1 -> 10))
        }
      })
    }
  }

  test("access refresh is a per-value maximum write age, not a background loader") {
    TestControl.executeEmbed(cache(access = true, refresh = Some(Duration.ofSeconds(15))).use { c =>
      for {
        calls  <- Ref.of[IO, Int](0)
        _      <- c.getOrUpdate(0, Set(1))(_ => calls.update(_ + 1).as(Map(1 -> 1)))
        _      <- IO.sleep(9.seconds) *> c.getOrUpdate(0, Set(1))(unexpected)
        _      <- IO.sleep(5.seconds) *> c.getOrUpdate(0, Set(1))(unexpected)
        before <- calls.get
        _      <- IO.sleep(1.second)
        at     <- c.getOrUpdate(0, Set(1))(_ => calls.update(_ + 1).as(Map(1 -> 2)))
        after  <- calls.get
      } yield { assertEquals(before, 1); assertEquals(after, 2); assertEquals(at, Map(1 -> 2)) }
    })
  }

  List(false, true).foreach { access =>
    test(s"$access: slow fill ages new B at publication without refreshing old A") {
      TestControl.executeEmbed(cache(access).use { c =>
        for {
          _       <- c.put(0, Map(1 -> 1))
          _       <- IO.sleep(4.seconds)
          _       <- c.getOrUpdate(0, Set(2))(_ => IO.sleep(4.seconds).as(Map(2 -> 2)))
          _       <- IO.sleep(2.seconds)
          asked   <- Ref.of[IO, Set[Int]](Set.empty)
          read    <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10)))
          missing <- asked.get
          _       <- IO.sleep(7.seconds) *> c.getOrUpdate(0, Set(2))(unexpected)
        } yield { assertEquals(missing, Set(1)); assertEquals(read, Map(1 -> 10, 2 -> 2)) }
      })
    }

    test(s"$access: explicit put overwrites only its keys; empty put does not refresh") {
      TestControl.executeEmbed(cache(access).use { c =>
        for {
          _       <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(1 -> 1)))
          _       <- IO.sleep(5.seconds) *> c.put(0, Map(2 -> 2))
          _       <- IO.sleep(4.seconds) *> c.put(0, Map.empty)
          _       <- IO.sleep(1.second)
          asked   <- Ref.of[IO, Set[Int]](Set.empty)
          read    <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10)))
          missing <- asked.get
        } yield { assertEquals(missing, Set(1)); assertEquals(read, Map(1 -> 10, 2 -> 2)) }
      })
    }

    List("left", "exception", "cancel").foreach { failure =>
      test(s"$access: $failure does not publish a touch or negative entries") {
        TestControl.executeEmbed(cache(access).use { c =>
          for {
            _       <- c.put(0, Map(1 -> 1))
            _       <- IO.sleep(9.seconds)
            failed  <- failure match {
                         case "left"      =>
                           c.getOrUpdateEither[String](0, Set(1, 2))(_ => IO.pure(Left("429")))
                             .map(r => assertEquals(r, Left("429")))
                         case "exception" =>
                           c.getOrUpdate(0, Set(1, 2))(_ => IO.raiseError(new IllegalStateException("provider")))
                             .attempt
                             .map(r => assert(r.isLeft))
                         case _           =>
                           for {
                             entered <- Deferred[IO, Unit]
                             f       <- c.getOrUpdate(0, Set(1, 2))(_ => entered.complete(()) *> IO.never).start
                             _       <- entered.get *> f.cancel
                             outcome <- f.join
                           } yield assert(outcome.isCanceled)
                       }
            _       <- IO.sleep(1.second)
            asked   <- Ref.of[IO, Set[Int]](Set.empty)
            _       <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10, 2 -> 20)))
            missing <- asked.get
          } yield assertEquals(missing, Set(1, 2))
        })
      }
    }

    test(s"$access: a slow response may return selected hits but cannot retain expired cells") {
      TestControl.executeEmbed(cache(access).use { c =>
        for {
          _        <- c.put(0, Map(1 -> 1))
          _        <- IO.sleep(9.seconds)
          response <- c.getOrUpdate(0, Set(1, 2))(_ => IO.sleep(11.seconds).as(Map(2 -> 2)))
          asked    <- Ref.of[IO, Set[Int]](Set.empty)
          _        <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10, 2 -> 20)))
          missing  <- asked.get
        } yield {
          assertEquals(response, Map(1 -> 1, 2 -> 2)); assertEquals(missing, if (access) Set(1, 2) else Set(1))
        }
      })
    }
  }

  test("write: selected A expires during B fill, but new B stays retained") {
    TestControl.executeEmbed(cache().use { c =>
      for {
        _       <- c.put(0, Map(1 -> 1))
        _       <- IO.sleep(5.seconds) *> c.put(0, Map(3 -> 3))
        _       <- IO.sleep(4.seconds)
        result  <- c.getOrUpdate(0, Set(1, 2))(_ => IO.sleep(2.seconds).as(Map(2 -> 2)))
        asked   <- Ref.of[IO, Set[Int]](Set.empty)
        _       <- c.getOrUpdate(0, Set(1, 2, 3))(keys => asked.set(keys).as(Map(1 -> 10)))
        missing <- asked.get
      } yield { assertEquals(result, Map(1 -> 1, 2 -> 2)); assertEquals(missing, Set(1)) }
    })
  }

  test("access: slow fill touch belongs to selection time, not completion time") {
    TestControl.executeEmbed(cache(access = true).use { c =>
      for {
        _       <- c.put(0, Map(1 -> 1))
        _       <- IO.sleep(4.seconds)
        _       <- c.getOrUpdate(0, Set(1, 2))(_ => IO.sleep(4.seconds).as(Map(2 -> 2)))
        _       <- IO.sleep(6.seconds)
        asked   <- Ref.of[IO, Set[Int]](Set.empty)
        _       <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10)))
        missing <- asked.get
      } yield assertEquals(missing, Set(1))
    })
  }

  List(false, true).foreach { empty =>
    test(s"result protocol and extra-key overwrite; cacheEmptyKeys=$empty") {
      TestControl.executeEmbed(cache(empty = empty).use { c =>
        for {
          calls   <- Ref.of[IO, List[Set[Int]]](Nil)
          nothing <- c.getOrUpdate(0, Set.empty)(_ => IO.raiseError(new AssertionError("empty request")))
          _       <- c.getOrUpdateOption(0, Set(1))(_ => IO.pure(None))
          _       <- c.getOrUpdate(0, Set(1))(keys => calls.update(_ :+ keys).as(Map(1 -> 1)))
          _       <- IO.sleep(5.seconds)
          partial <- c.getOrUpdate(0, Set(2, 3))(keys => calls.update(_ :+ keys).as(Map(1 -> 10, 2 -> 2, 4 -> 4)))
          _       <- IO.sleep(5.seconds)
          read    <- c.getOrUpdate(0, Set(1, 2, 3, 4))(keys => calls.update(_ :+ keys).as(Map(3 -> 3)))
          seen    <- calls.get
        } yield {
          assertEquals(nothing, Map.empty[Int, Int]); assertEquals(partial, Map(2 -> 2))
          assertEquals(read, if (empty) Map(1 -> 10, 2 -> 2, 4 -> 4) else Map(1 -> 10, 2 -> 2, 3 -> 3, 4 -> 4))
          assertEquals(seen, if (empty) List(Set(1), Set(2, 3)) else List(Set(1), Set(2, 3), Set(3)))
        }
      })
    }
  }

  test("access hit waits for the group fill, then rechecks age; other groups stay independent") {
    TestControl.executeEmbed(cache(access = true).use { c =>
      for {
        _       <- c.put(0, Map(1 -> 1))
        _       <- IO.sleep(8.seconds)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        done    <- Deferred[IO, Unit]
        owner   <- c.getOrUpdate(0, Set(2))(_ => entered.complete(()) *> release.get.as(Map(2 -> 2))).start
        _       <- entered.get
        waiter  <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 10))).flatTap(_ => done.complete(())).start
        other   <- c.getOrUpdate(1, Set(1))(_ => IO.pure(Map(1 -> 100)))
        _       <- IO.sleep(3.seconds)
        pending <- done.tryGet
        _       <- release.complete(()) *> owner.joinWithNever
        value   <- waiter.joinWithNever
      } yield { assertEquals(pending, None); assertEquals(value, Map(1 -> 10)); assertEquals(other, Map(1 -> 100)) }
    })
  }

  List(false, true).foreach { access =>
    test(s"$access: overlapping and disjoint waiters load only the still-missing keys") {
      TestControl.executeEmbed(cache(access).use { c =>
        for {
          entered  <- Deferred[IO, Unit]
          release  <- Deferred[IO, Unit]
          asked    <- Ref.of[IO, List[Set[Int]]](Nil)
          owner    <- c.getOrUpdate(0, Set(1, 2))(_ => entered.complete(()) *> release.get.as(Map(1 -> 1, 2 -> 2))).start
          _        <- entered.get
          overlap  <- c.getOrUpdate(0, Set(2, 3))(keys => asked.update(_ :+ keys).as(Map(3 -> 3))).start
          disjoint <- c.getOrUpdate(0, Set(4))(keys => asked.update(_ :+ keys).as(Map(4 -> 4))).start
          _        <- IO.cede *> release.complete(()) *> owner.joinWithNever
          a        <- overlap.joinWithNever
          b        <- disjoint.joinWithNever
          missing  <- asked.get
        } yield {
          assertEquals(a, Map(2 -> 2, 3 -> 3)); assertEquals(b, Map(4 -> 4))
          assertEquals(missing.toSet, Set(Set(3), Set(4)))
        }
      })
    }

    test(s"$access: failed owner does not force its error onto a waiting caller") {
      TestControl.executeEmbed(cache(access).use { c =>
        for {
          entered <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          owner   <-
            c.getOrUpdateEither[String](0, Set(1))(_ => entered.complete(()) *> release.get.as(Left("429"))).start
          _       <- entered.get
          waiter  <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 1))).start
          _       <- IO.cede *> release.complete(())
          failure <- owner.joinWithNever
          value   <- waiter.joinWithNever
        } yield { assertEquals(failure, Left("429")); assertEquals(value, Map(1 -> 1)) }
      })
    }
  }

  test("access: invalidation during a touched fill fences both timestamps and values") {
    TestControl.executeEmbed(cache(access = true).use { c =>
      for {
        _       <- c.put(0, Map(1 -> 1))
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        old     <- c.getOrUpdate(0, Set(1, 2))(_ => entered.complete(()) *> release.get.as(Map(2 -> 2))).start
        _       <- entered.get *> c.invalidate(0) *> c.put(0, Map(1 -> 10))
        _       <- release.complete(()) *> old.joinWithNever
        read    <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(2 -> 20)))
      } yield assertEquals(read, Map(1 -> 10, 2 -> 20))
    })
  }

  test("invalid durations and negative capacity fail allocation before any loader") {
    val invalid = List(Duration.ofNanos(-1), Duration.ofSeconds(Long.MaxValue))
    (invalid.flatMap(d =>
      List(
        SetCache.expireAfterWrite[IO, Int, Int, Int]("bad", d, 1L, true),
        SetCache.expireAfterAccess[IO, Int, Int, Int]("bad", d, 1L, true),
        SetCache.expireAfterAccess[IO, Int, Int, Int]("bad", ttl, Some(d), 1L, true)
      )
    ) :+ SetCache.expireAfterWrite[IO, Int, Int, Int]("bad", ttl, -1L, true)).traverse_ { resource =>
      resource.use(_ => IO.raiseError(new AssertionError("allocated invalid policy"))).attempt.map { result =>
        assert(result.left.exists(e => e.isInstanceOf[IllegalArgumentException] || e.isInstanceOf[ArithmeticException]))
      }
    }
  }

  test("zero access refresh retains nothing and terminates") {
    TestControl.executeEmbed(cache(access = true, refresh = Some(Duration.ZERO)).use { c =>
      for {
        _      <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map.empty))
        result <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 1)))
      } yield assertEquals(result, Map(1 -> 1))
    })
  }

  List(false, true).foreach { access =>
    test(s"$access: sequential read/put/time/invalidate agrees with a per-value reference model") {
      TestControl.executeEmbed(cache(access).use { c =>
        // Independent reference: integer seconds and absolute expiry, small fixed sequence, no cache internals.
        val commands = List(
          ("put", 1),
          ("tick", 4),
          ("read", 1),
          ("put", 2),
          ("tick", 6),
          ("read", 2),
          ("read", 1),
          ("tick", 10),
          ("read", 1),
          ("clear", 0),
          ("read", 2)
        )
        commands
          .foldLeftM((0, Map.empty[Int, (Int, Int)])) { case ((now, model), (op, key)) =>
            op match {
              case "tick"  => IO.sleep(key.seconds).as((now + key, model))
              case "clear" => c.invalidate(0).as((now, Map.empty[Int, (Int, Int)]))
              case "put"   => c.put(0, Map(key -> key)).as((now, model.updated(key, (key, now + 10))))
              case _       =>
                val hit      = model.get(key).filter(_._2 > now)
                val expected = hit.fold(key * 100)(_._1)
                c.getOrUpdate(0, Set(key))(_ => IO.pure(Map(key -> (key * 100)))).map { value =>
                  assertEquals(value, Map(key -> expected))
                  val deadline = if (access || hit.isEmpty) now + 10 else hit.get._2
                  (now, model.updated(key, (expected, deadline)))
                }
            }
          }
          .void
      })
    }
  }

  List(false, true).foreach { access =>
    List(false, true).foreach { negative =>
      List(17.seconds, 18.seconds, 19.seconds).foreach { observed =>
        test(s"$access: slow B publication boundary at $observed; negative=$negative") {
          TestControl.executeEmbed(cache(access).use { c =>
            for {
              _      <- c.put(0, Map(1 -> 1))
              _      <- IO.sleep(4.seconds)
              _      <- c.getOrUpdate(0, Set(2))(_ => IO.sleep(4.seconds).as(if (negative) Map.empty else Map(2 -> 2)))
              _      <- IO.sleep(observed - 8.seconds)
              calls  <- Ref.of[IO, Int](0)
              result <- c.getOrUpdate(0, Set(2))(_ => calls.update(_ + 1).as(Map(2 -> 20)))
              count  <- calls.get
            } yield {
              assertEquals(count, if (observed < 18.seconds) 0 else 1)
              assertEquals(
                result,
                if (observed < 18.seconds) if (negative) Map.empty[Int, Int] else Map(2 -> 2) else Map(2 -> 20)
              )
            }
          })
        }
      }
    }
  }

  test("access: concurrent hit touches compose without losing either key's timestamp") {
    TestControl.executeEmbed(cache(access = true).use { c =>
      for {
        _       <- c.put(0, Map(1 -> 1, 2 -> 2))
        _       <- IO.sleep(9.seconds)
        pair    <- (c.getOrUpdate(0, Set(1))(unexpected), c.getOrUpdate(0, Set(2))(unexpected)).parTupled
        _       <- IO.sleep(1.second)
        both    <- c.getOrUpdate(0, Set(1, 2))(unexpected)
        _       <- IO.sleep(10.seconds)
        asked   <- Ref.of[IO, Set[Int]](Set.empty)
        _       <- c.getOrUpdate(0, Set(1, 2))(keys => asked.set(keys).as(Map(1 -> 10, 2 -> 20)))
        expired <- asked.get
      } yield {
        assertEquals(pair, (Map(1 -> 1), Map(2 -> 2)))
        assertEquals(both, Map(1 -> 1, 2 -> 2)); assertEquals(expired, Set(1, 2))
      }
    })
  }

  test("access: expired selected hits are removed from capacity while a waiting caller keeps the group alive") {
    TestControl.executeEmbed(cache(access = true, maximum = 2L).use { c =>
      for {
        _        <- c.put(0, Map(1 -> 1))
        _        <- IO.sleep(9.seconds)
        entered  <- Deferred[IO, Unit]
        release  <- Deferred[IO, Unit]
        owner    <- c.getOrUpdate(0, Set(1, 2))(_ => entered.complete(()) *> release.get.as(Map(2 -> 2))).start
        _        <- entered.get *> IO.sleep(7.seconds)
        waiter   <- c.getOrUpdate(0, Set(3))(_ => IO.pure(Map(3 -> 3))).start
        _        <- IO.sleep(4.seconds) *> release.complete(())
        response <- owner.joinWithNever
        _        <- waiter.joinWithNever
        retained <- c.getOrUpdate(0, Set(2, 3))(unexpected)
        fresh    <- c.getOrUpdate(0, Set(1))(_ => IO.pure(Map(1 -> 10)))
      } yield {
        assertEquals(response, Map(1 -> 1, 2 -> 2))
        assertEquals(retained, Map(2 -> 2, 3 -> 3)); assertEquals(fresh, Map(1 -> 10))
      }
    })
  }

  test("access: canceling a waiting hit does not cancel the owner or publish a touch") {
    TestControl.executeEmbed(cache(access = true).use { c =>
      for {
        _           <- c.put(0, Map(1 -> 1))
        _           <- IO.sleep(8.seconds)
        entered     <- Deferred[IO, Unit]
        release     <- Deferred[IO, Unit]
        canceled    <- Ref.of[IO, Boolean](false)
        owner       <- c.getOrUpdate(0, Set(2))(_ =>
                         (entered.complete(()) *> release.get.as(Map(2 -> 2))).onCancel(canceled.set(true))
                       ).start
        _           <- entered.get
        waiter      <- c.getOrUpdate(0, Set(1))(unexpected).start
        _           <- IO.sleep(1.second) *> waiter.cancel *> release.complete(())
        loaded      <- owner.joinWithNever
        _           <- IO.sleep(1.second)
        read        <- c.getOrUpdate(0, Set(1, 2))(_ => IO.pure(Map(1 -> 10)))
        wasCanceled <- canceled.get
        outcome     <- waiter.join
      } yield {
        assert(!wasCanceled); assert(outcome.isCanceled)
        assertEquals(loaded, Map(2 -> 2)); assertEquals(read, Map(1 -> 10, 2 -> 2))
      }
    })
  }

  test("SetCache Resource owns its metric fiber and keeps bounded weight gauges") {
    TestControl.executeEmbed {
      for {
        records     <- Ref.of[IO, List[(String, Double)]](Nil)
        metrics      = new CacheMetrics[IO] {
                         def path: IO[String] = IO.pure("/synthetic")
                         def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
                         def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] =
                           records.update(_ :+ (name -> value))
                       }
        allocated   <-
          SetCache.expireAfterWrite[IO, Int, Int, Int]("lifecycle", Duration.ofDays(1), 5L, true, metrics).allocated
        (c, release) = allocated
        before      <- (c.put(0, Map(1 -> 1)) *> IO.sleep(31.seconds) *> records.get).guarantee(release)
        _           <- IO.sleep(31.seconds)
        after       <- records.get
      } yield {
        assert(before.contains("cache_weight" -> 1.0)); assert(before.contains("cache_max_weight" -> 5.0))
        assertEquals(after, before)
      }
    }
  }
}
