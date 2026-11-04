package com.parrot669.catsutils.cache

import java.time.{Duration => JDuration}
import scala.concurrent.duration._

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import munit.CatsEffectSuite

/** The first twenty scenarios port the supplied CacheTest; later tests cover conversion regressions. */
class CacheTest extends CatsEffectSuite {
  private val day   = JDuration.ofDays(1)
  private val five  = JDuration.ofSeconds(5)
  private def cache = Cache.expireAfterWrite[Int, Nothing, String]("test", day)
  private def virtual[A](program: IO[A]): IO[A] = TestControl.executeEmbed(program)
  private val boom = new RuntimeException("boom!")

  test("get previously put value") {
    cache.use(c =>
      for {
        absent <- c.get(0)
        _       = assertEquals(absent, None)
        _      <- c.put(0, "test")
        value  <- c.get(0)
      } yield assertEquals(value, Some("test"))
    )
  }

  test("expire values after write") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      c.put(0, "test") *> IO.sleep(5.seconds) *> c.get(0).map(v => assertEquals(v, None))
    })
  }

  test("expire values after access") {
    virtual(Cache.expireAfterAccess[Int, Nothing, String]("test", five).use { c =>
      for {
        _      <- c.put(0, "test")
        _      <- IO.sleep(3.seconds) *> c.get(0)
        first  <- IO.sleep(3.seconds) *> c.get(0)
        second <- IO.sleep(5.seconds) *> c.get(0)
      } yield { assertEquals(first, Some("test")); assertEquals(second, None) }
    })
  }

  test("invalidate values manually") {
    cache.use(c => c.put(0, "test") *> c.invalidate(0) *> c.get(0).map(v => assertEquals(v, None)))
  }

  test("do not run update function if value is already present") {
    cache.use(c => c.put(0, "test") *> c.getOrUpdateValue(0)(IO.raiseError(boom)).map(v => assertEquals(v, "test")))
  }

  test("run update if value is missing") {
    cache.use(_.getOrUpdateValue(0)(IO.pure("test")).map(v => assertEquals(v, "test")))
  }

  test("wait for pending update to finish on get") {
    virtual(cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> IO.sleep(5.seconds).as("test")).start
        _       <- started.get
        value   <- c.get(0)
        _       <- leader.joinWithNever
      } yield assertEquals(value, Some("test"))
    })
  }

  test("catch update errors on get") {
    virtual(cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c
                     .getOrUpdateValue(0)(started.complete(()) *> IO.sleep(5.seconds) *> IO.raiseError(boom))
                     .attempt
                     .start
        _       <- started.get
        value   <- c.get(0)
        failed  <- leader.joinWithNever
      } yield { assertEquals(value, None); assertEquals(failed, Left(boom)) }
    })
  }

  test("do not cache failed updates") {
    cache.use { c =>
      for {
        failed <- c.getOrUpdateValue(0)(IO.raiseError(boom)).attempt
        value  <- c.getOrUpdateValue(0)(IO.pure("test"))
      } yield { assertEquals(failed, Left(boom)); assertEquals(value, "test") }
    }
  }

  test("cache business errors") {
    Cache.expireAfterWrite[Int, String, String]("test", day).use { c =>
      for {
        first  <- c.getOrUpdate(0)(IO.pure(Left("boom!")))
        second <- c.getOrUpdate(0)(IO.pure(Right("test")))
      } yield { assertEquals(first, Left("boom!")); assertEquals(second, Left("boom!")) }
    }
  }

  test("pass dependencies to update via an explicit closure") {
    val context = "test"
    cache.use(
      _.getOrUpdateValue(0)(IO.pure(if (context == "test") "success" else "failure"))
        .map(v => assertEquals(v, "success"))
    )
  }

  test("expire loaded values after write") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      c.getOrUpdateValue(0)(IO.pure("test")) *> IO.sleep(5.seconds) *> c.get(0).map(v => assertEquals(v, None))
    })
  }

  test("expire loaded values after access") {
    virtual(Cache.expireAfterAccess[Int, Nothing, String]("test", five).use { c =>
      for {
        _      <- c.getOrUpdateValue(0)(IO.pure("test"))
        _      <- IO.sleep(3.seconds) *> c.get(0)
        first  <- IO.sleep(3.seconds) *> c.get(0)
        second <- IO.sleep(5.seconds) *> c.get(0)
      } yield { assertEquals(first, Some("test")); assertEquals(second, None) }
    })
  }

  test("run at most one update for a resident key") {
    virtual(cache.use { c =>
      for {
        count  <- Ref.of[IO, Int](0)
        values <- List.fill(5)(c.getOrUpdateValue(0)(IO.sleep(3.seconds) *> count.update(_ + 1).as("test"))).parSequence
        actual <- count.get
      } yield { assertEquals(values, List.fill(5)("test")); assertEquals(actual, 1) }
    })
  }

  test("do not cache None values") {
    cache.use { c =>
      for {
        first  <- c.getOrUpdateOptionValue(0)(IO.pure(None))
        second <- c.getOrUpdateOptionValue(0)(IO.pure(Some("test")))
        third  <- c.getOrUpdateOptionValue(0)(IO.pure(Some("changed")))
      } yield { assertEquals(first, None); assertEquals(second, Some("test")); assertEquals(third, Some("test")) }
    }
  }

  test("do not evaluate update effect unless necessary") {
    cache.use(c =>
      c.getOrUpdateValue(0)(IO.pure("test")) *>
        c.getOrUpdateValue(0)(throw boom).map(v => assertEquals(v, "test"))
    )
  }

  test("retry after initial update cancellation") {
    cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        fiber   <- c.getOrUpdateValue(0)(started.complete(()) *> IO.never).start
        _       <- started.get *> fiber.cancel
        value   <- c.getOrUpdateValue(0)(IO.pure("other"))
      } yield assertEquals(value, "other")
    }
  }

  test("waiter cancellation does not cancel initial loader") {
    virtual(cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> IO.sleep(10.seconds).as("test")).start
        _       <- started.get
        waiter  <- c.getOrUpdateValue(0)(IO.pure("killme")).start
        _       <- IO.sleep(1.nano) *> waiter.cancel
        value   <- leader.joinWithNever
      } yield assertEquals(value, "test")
    })
  }

  test("expire value via refresh option") {
    virtual(Cache.expireAfterAccess[Int, Nothing, String]("test", five, Some(JDuration.ofSeconds(12))).use { c =>
      for {
        _      <- c.getOrUpdateValue(0)(IO.pure("test"))
        _      <- IO.sleep(4.seconds) *> c.get(0)
        _      <- IO.sleep(4.seconds) *> c.get(0)
        first  <- IO.sleep(3.seconds) *> c.getOrUpdateValue(0)(IO.pure("test-refreshed"))
        second <- IO.sleep(2.seconds) *> c.getOrUpdateValue(0)(IO.pure("test-refreshed"))
      } yield { assertEquals(first, "test"); assertEquals(second, "test-refreshed") }
    })
  }

  test("expire value via expiration option") {
    virtual(Cache.expireAfterAccess[Int, Nothing, String]("test", five, Some(JDuration.ofSeconds(11))).use { c =>
      c.getOrUpdateValue(0)(IO.pure("test")) *> IO.sleep(6.seconds) *> c.get(0).map(v => assertEquals(v, None))
    })
  }

  // Added Cats Effect cancellation, publication, expiry and weight regressions.
  test("joined waiter retries its own loader when leader is canceled") {
    virtual(cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> IO.never).start
        _       <- started.get
        waiter  <- c.getOrUpdateValue(0)(IO.pure("retry")).start
        _       <- IO.sleep(1.nano) *> leader.cancel
        actual  <- waiter.joinWithNever
        stored  <- c.get(0)
      } yield { assertEquals(actual, "retry"); assertEquals(stored, Some("retry")) }
    })
  }

  test("get cancellation is preserved and leaves the loader running") {
    virtual(cache.use { c =>
      for {
        started  <- Deferred[IO, Unit]
        leader   <- c.getOrUpdateValue(0)(started.complete(()) *> IO.sleep(1.second).as("test")).start
        _        <- started.get
        reader   <- c.get(0).start
        _        <- IO.sleep(1.nano) *> reader.cancel
        canceled <- reader.join
        value    <- leader.joinWithNever
      } yield { assert(canceled.isCanceled); assertEquals(value, "test") }
    })
  }

  test("throwing while constructing a loader releases all waiters and permits retry") {
    cache.use { c =>
      for {
        failure <- c.getOrUpdateValue(0)(throw boom).attempt
        retry   <- c.getOrUpdateValue(0)(IO.pure("retry"))
      } yield { assertEquals(failure, Left(boom)); assertEquals(retry, "retry") }
    }
  }

  test("business errors are invisible to get and expire normally") {
    virtual(Cache.expireAfterWrite[Int, String, String]("test", five).use { c =>
      for {
        _      <- c.getOrUpdate(0)(IO.pure(Left("business")))
        hidden <- c.get(0)
        _      <- IO.sleep(5.seconds)
        retry  <- c.getOrUpdate(0)(IO.pure(Right("retry")))
      } yield { assertEquals(hidden, None); assertEquals(retry, Right("retry")) }
    })
  }

  test("value waiter retries after an optional leader returns None") {
    virtual(cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateOptionValue(0)(started.complete(()) *> IO.sleep(1.second).as(None)).start
        _       <- started.get
        value   <- c.getOrUpdateValue(0)(IO.pure("retry"))
        empty   <- leader.joinWithNever
      } yield { assertEquals(empty, None); assertEquals(value, "retry") }
    })
  }

  test("invalidation during a load prevents stale republishing") {
    cache.use { c =>
      for {
        started  <- Deferred[IO, Unit]
        finish   <- Deferred[IO, Unit]
        leader   <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get.as("old")).start
        _        <- started.get *> c.invalidate(0) *> finish.complete(())
        original <- leader.joinWithNever
        stored   <- c.get(0)
      } yield { assertEquals(original, "old"); assertEquals(stored, None) }
    }
  }

  test("older load cannot overwrite a newer explicit put") {
    cache.use { c =>
      for {
        started  <- Deferred[IO, Unit]
        finish   <- Deferred[IO, Unit]
        leader   <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get.as("old")).start
        _        <- started.get *> c.put(0, "new") *> finish.complete(())
        original <- leader.joinWithNever
        stored   <- c.get(0)
      } yield { assertEquals(original, "old"); assertEquals(stored, Some("new")) }
    }
  }

  test("older failed loader cannot invalidate a replacement") {
    cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        finish  <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get *> IO.raiseError(boom)).attempt.start
        _       <- started.get *> c.put(0, "new") *> finish.complete(())
        failed  <- leader.joinWithNever
        stored  <- c.get(0)
      } yield { assertEquals(failed, Left(boom)); assertEquals(stored, Some("new")) }
    }
  }

  test("older canceled loader cannot invalidate a replacement") {
    cache.use { c =>
      for {
        started <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> IO.never).start
        _       <- started.get *> c.put(0, "new") *> leader.cancel
        stored  <- c.get(0)
      } yield assertEquals(stored, Some("new"))
    }
  }

  test("expired loading entry does not overwrite a new generation") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      for {
        started <- Deferred[IO, Unit]
        finish  <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get.as("old")).start
        _       <- started.get *> IO.sleep(5.seconds)
        fresh   <- c.getOrUpdateValue(0)(IO.pure("new"))
        _       <- finish.complete(()) *> leader.joinWithNever
        stored  <- c.get(0)
      } yield { assertEquals(fresh, "new"); assertEquals(stored, Some("new")) }
    })
  }

  test("weighted values stay within capacity and errors weigh one") {
    Cache.expireAfterWrite[Int, String, String]("weighted", day, 5L, (value: String) => value.length).use { c =>
      for {
        _         <- c.put(0, "abc")
        initial   <- c.weight
        _         <- c.getOrUpdate(1)(IO.pure(Left("long error text")))
        withError <- c.weight
        _         <- c.put(2, "defg")
        evicted   <- c.weight
      } yield { assertEquals(initial, 3L); assertEquals(withError, 4L); assert(evicted <= 5L) }
    }
  }

  test("a slow loader receives a fresh expiration lifetime after completion") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      for {
        value   <- c.getOrUpdateValue(0)(IO.sleep(10.seconds).as("slow"))
        cached  <- c.get(0)
        _       <- IO.sleep(5.seconds)
        expired <- c.get(0)
      } yield { assertEquals(value, "slow"); assertEquals(cached, Some("slow")); assertEquals(expired, None) }
    })
  }

  test("invalidation retires an already expired loading generation") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      for {
        started <- Deferred[IO, Unit]
        finish  <- Deferred[IO, Unit]
        leader  <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get.as("old")).start
        _       <- started.get *> IO.sleep(6.seconds) *> c.invalidate(0) *> finish.complete(())
        _       <- leader.joinWithNever
        stored  <- c.get(0)
      } yield assertEquals(stored, None)
    })
  }

  test("zero-weight values can be cached after a loading placeholder is evicted") {
    Cache.expireAfterWrite[Int, Nothing, String]("zero", day, 0L, (_: String) => 0).use { c =>
      for {
        loaded <- c.getOrUpdateValue(0)(IO.pure("empty"))
        cached <- c.get(0)
        weight <- c.weight
      } yield { assertEquals(loaded, "empty"); assertEquals(cached, Some("empty")); assertEquals(weight, 0L) }
    }
  }

  test("invalid weight configuration fails within resource acquisition") {
    Cache.make[Int, Nothing, String]("bad", Some(day), None, Some(1L), None).use(_ => IO.unit).attempt.map {
      case Left(_: IllegalArgumentException) => ()
      case other                             => fail(s"Expected validation error, got $other")
    }
  }

  test("throwing publication weigher releases waiters and allows retry") {
    virtual(
      Cache
        .expireAfterWrite[Int, Nothing, String](
          "weigher",
          day,
          10L,
          (value: String) => if (value == "bad") throw boom else value.length
        )
        .use { c =>
          for {
            started <- Deferred[IO, Unit]
            leader  <- c.getOrUpdateValue(0)(started.complete(()) *> IO.sleep(1.second).as("bad")).attempt.start
            _       <- started.get
            waiter  <- c.getOrUpdateValue(0)(IO.pure("unused")).attempt
            first   <- leader.joinWithNever
            retry   <- c.getOrUpdateValue(0)(IO.pure("good"))
            cached  <- c.get(0)
          } yield {
            assertEquals(first, Left(boom)); assertEquals(waiter, Left(boom)); assertEquals(retry, "good");
            assertEquals(cached, Some("good"))
          }
        }
    )
  }

  test("negative publication weight fails without poisoning subsequent loads") {
    Cache
      .expireAfterWrite[Int, Nothing, String](
        "weigher",
        day,
        10L,
        (value: String) => if (value == "bad") -1 else value.length
      )
      .use { c =>
        for {
          failed <- c.getOrUpdateValue(0)(IO.pure("bad")).attempt
          retry  <- c.getOrUpdateValue(0)(IO.pure("good"))
        } yield { assert(failed.left.exists(_.isInstanceOf[IllegalArgumentException])); assertEquals(retry, "good") }
      }
  }

  test("failed explicit put preserves an active loader and its publication") {
    Cache
      .expireAfterWrite[Int, Nothing, String](
        "weigher",
        day,
        10L,
        (value: String) => if (value == "bad") throw boom else value.length
      )
      .use { c =>
        for {
          started <- Deferred[IO, Unit]
          finish  <- Deferred[IO, Unit]
          leader  <- c.getOrUpdateValue(0)(started.complete(()) *> finish.get.as("good")).start
          _       <- started.get
          failed  <- c.put(0, "bad").attempt
          _       <- finish.complete(()) *> leader.joinWithNever
          cached  <- c.get(0)
        } yield { assertEquals(failed, Left(boom)); assertEquals(cached, Some("good")) }
      }
  }

  test("keys and size remove expired entries") {
    virtual(Cache.expireAfterWrite[Int, Nothing, String]("test", five).use { c =>
      for {
        _     <- c.put(0, "a") *> c.put(1, "b")
        keys  <- c.keys
        size  <- c.size
        _     <- IO.sleep(5.seconds)
        empty <- c.keys
        zero  <- c.size
      } yield {
        assertEquals(keys, Set(0, 1)); assertEquals(size, 2L); assertEquals(empty, Set.empty[Int]);
        assertEquals(zero, 0L)
      }
    })
  }
}
