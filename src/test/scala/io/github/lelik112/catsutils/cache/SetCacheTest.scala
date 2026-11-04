// Source scenarios are from the owner-supplied Bobylev utility tests; see PROVENANCE.md.
package io.github.lelik112.catsutils.cache

import java.time.Duration
import scala.concurrent.duration._

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.syntax.all._
import cats.effect.testkit.TestControl
import cats.syntax.all._
import munit.CatsEffectSuite

class SetCacheTest extends CatsEffectSuite {
  private def cache(empty: Boolean = false): Resource[IO, SetCache[String, String, Int]] =
    SetCache.expireAfterWrite[String, String, Int]("test", Duration.ofMinutes(1), cacheEmptyKeys = empty)

  test("[source] stores and returns values. cacheEmptyKeys = false") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1, "2" -> 2, "3" -> 3))
        empty <- cache.getOrUpdate("test", Set("669"))(_ => IO.pure(Map.empty))
        read0 <- cache.getOrUpdate("test", Set("669"))(_ => IO.pure(Map("669" -> 669)))
        read1 <- cache.getOrUpdateOption("test", Set("1"))(_ => IO.pure(None))
        read2 <- cache.getOrUpdate("test", Set("1", "2"))(_ => IO.pure(Map("2" -> 669)))
      } yield {
        assert(empty.isEmpty)
        assertEquals(read0, Map("669" -> 669))
        assertEquals(read1, Map("1" -> 1))
        assertEquals(read2, Map("1" -> 1, "2" -> 2))
      }
    })
  }

  test("[source] not replace empty value when cacheEmptyKeys = true") {
    TestControl.executeEmbed(cache(empty = true).use { cache =>
      for {
        _ <- cache.getOrUpdate("test", Set("669"))(_ => IO.pure(Map.empty))
        read <- cache.getOrUpdate("test", Set("669"))(_ => IO.pure(Map("669" -> 669)))
      } yield assert(read.isEmpty)
    })
  }

  test("[source] not replace with empty value in the second call") {
    TestControl.executeEmbed(cache(empty = true).use { cache =>
      for {
        _ <- cache.getOrUpdate("test", Set("669"))(_ => IO.pure(Map("669" -> 669)))
        read1 <- cache.getOrUpdate("test", Set("669", "34"))(_ => IO.pure(Map("34" -> 34)))
        read2 <- cache.getOrUpdate("test", Set("669", "34"))(_ => IO.pure(Map.empty))
      } yield {
        assertEquals(read1, Map("669" -> 669, "34" -> 34))
        assertEquals(read2, Map("669" -> 669, "34" -> 34))
      }
    })
  }

  test("[source] replaces values") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1, "2" -> 2, "3" -> 3))
        _ <- cache.put("test", Map("3" -> 669))
        read <- cache.getOrUpdate("test", Set("1", "2", "3"))(_ => IO.pure(Map.empty))
      } yield assertEquals(read, Map("1" -> 1, "2" -> 2, "3" -> 669))
    })
  }

  test("[source] waits for the load to finish before putting") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        loading <- cache
          .getOrUpdate("test", Set("1", "2", "3")) { _ =>
            started.complete(()) *> release.get.as(Map("1" -> 1, "2" -> 2, "3" -> 3))
          }
          .start
        _ <- started.get
        putting <- cache.put("test", Map("1" -> 669, "5" -> 5)).start
        _ <- IO.cede *> release.complete(())
        _ <- loading.joinWithNever *> putting.joinWithNever
        read <- cache.getOrUpdate("test", Set("1", "2", "3", "5"))(_ => IO.pure(Map.empty))
      } yield assertEquals(read, Map("1" -> 669, "2" -> 2, "3" -> 3, "5" -> 5))
    })
  }

  test("[source] invalidates data") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1, "2" -> 2, "3" -> 3))
        _ <- cache.invalidate("test")
        read <- cache.getOrUpdate("test", Set("1", "2", "3"))(_ => IO.pure(Map.empty))
      } yield assert(read.isEmpty)
    })
  }

  test("[source] invalidates ongoing load") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        loading <- cache
          .getOrUpdate("test", Set("1", "2", "3")) { _ =>
            started.complete(()) *> release.get.as(Map("1" -> 1, "2" -> 2, "3" -> 3))
          }
          .start
        _ <- started.get *> cache.invalidate("test") *> release.complete(())
        _ <- loading.joinWithNever
        read <- cache.getOrUpdate("test", Set("1", "2", "3"))(_ => IO.pure(Map.empty))
      } yield assert(read.isEmpty)
    })
  }

  test("[source] is stable of failure") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1, "2" -> 2, "3" -> 3))
        _ <- cache
          .getOrUpdate("test", Set("1", "2", "3"))(_ => IO.raiseError(new RuntimeException("BOOM")))
          .handleError(_ => Map.empty)
        read <- cache.getOrUpdate("test", Set("1", "2", "3"))(_ => IO.pure(Map.empty))
      } yield assertEquals(read, Map("1" -> 1, "2" -> 2, "3" -> 3))
    })
  }

  test("[cats-effect] empty requests never invoke the loader") {
    cache()
      .use(_.getOrUpdate("test", Set.empty)(_ => IO.raiseError(new AssertionError("unexpected loader"))))
      .map(result => assertEquals(result, Map.empty[String, Int]))
  }

  test("[cats-effect] business errors preserve cached keys and allow retry") {
    TestControl.executeEmbed(cache(empty = true).use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1))
        failed <- cache.getOrUpdateEither[String]("test", Set("1", "2")) { missing =>
          IO(assertEquals(missing, Set("2"))).as(Left("unavailable"))
        }
        retried <- cache.getOrUpdate("test", Set("1", "2"))(_ => IO.pure(Map("2" -> 2)))
      } yield {
        assertEquals(failed, Left("unavailable"))
        assertEquals(retried, Map("1" -> 1, "2" -> 2))
      }
    })
  }

  test("[cats-effect] raised loader exceptions release the lock and preserve prior values") {
    TestControl.executeEmbed(cache().use { cache =>
      val boom = new RuntimeException("loader failure")
      for {
        _ <- cache.put("test", Map("1" -> 1))
        failed <- cache.getOrUpdate("test", Set("1", "2"))(_ => IO.raiseError(boom)).attempt
        retried <- cache.getOrUpdate("test", Set("1", "2"))(_ => IO.pure(Map("2" -> 2)))
      } yield {
        assertEquals(failed, Left(boom))
        assertEquals(retried, Map("1" -> 1, "2" -> 2))
      }
    })
  }

  test("[cats-effect] canceling a loader releases its permit") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        loading <- cache.getOrUpdate("test", Set("1"))(_ => started.complete(()) *> IO.never).start
        _ <- started.get *> loading.cancel
        read <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2)))
      } yield assertEquals(read, Map("1" -> 2))
    })
  }

  test("[cats-effect] canceling a waiter leaves the active load running") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        canceled <- Ref.of[IO, Boolean](false)
        owner <- cache
          .getOrUpdate("test", Set("1")) { _ =>
            (started.complete(()) *> release.get.as(Map("1" -> 1))).onCancel(canceled.set(true))
          }
          .start
        _ <- started.get
        waiter <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2))).start
        _ <- IO.cede *> waiter.cancel *> release.complete(())
        result <- owner.joinWithNever
        didCancel <- canceled.get
        read <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 3)))
      } yield {
        assert(!didCancel)
        assertEquals(result, Map("1" -> 1))
        assertEquals(read, Map("1" -> 1))
      }
    })
  }

  test("[cats-effect] stale completion cannot overwrite a replacement after invalidation") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        old <- cache.getOrUpdate("test", Set("1"))(_ => started.complete(()) *> release.get.as(Map("1" -> 1))).start
        _ <- started.get *> cache.invalidate("test")
        fresh <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2)))
        _ <- release.complete(()) *> old.joinWithNever
        read <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 3)))
      } yield {
        assertEquals(fresh, Map("1" -> 2))
        assertEquals(read, Map("1" -> 2))
      }
    })
  }

  test("[cats-effect] stale completion cannot overwrite an entry replaced after expiration") {
    TestControl.executeEmbed(cache().use { cache =>
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        old <- cache.getOrUpdate("test", Set("1"))(_ => started.complete(()) *> release.get.as(Map("1" -> 1))).start
        _ <- started.get *> IO.sleep(61.seconds)
        _ <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2)))
        _ <- release.complete(()) *> old.joinWithNever
        read <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 3)))
      } yield assertEquals(read, Map("1" -> 2))
    })
  }

  test("[cats-effect] None does not negatively cache keys and extra returned keys are cached") {
    TestControl.executeEmbed(cache(empty = true).use { cache =>
      for {
        none <- cache.getOrUpdateOption("test", Set("1"))(_ => IO.pure(None))
        first <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 1, "2" -> 2)))
        second <- cache.getOrUpdate("test", Set("2"))(_ => IO.raiseError(new AssertionError("unexpected loader")))
      } yield {
        assert(none.isEmpty)
        assertEquals(first, Map("1" -> 1))
        assertEquals(second, Map("2" -> 2))
      }
    })
  }

  List(
    SetCache.expireAfterWrite[String, String, Int]("test", Duration.ZERO, cacheEmptyKeys = false),
    SetCache.expireAfterAccess[String, String, Int]("test", Duration.ZERO, cacheEmptyKeys = false)
  ).zipWithIndex.foreach { case (resource, index) =>
    test(s"[cats-effect] zero expiration mode $index completes requests without retaining data") {
      TestControl.executeEmbed(resource.use { cache =>
        for {
          first <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 1)))
          second <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2)))
        } yield {
          assertEquals(first, Map("1" -> 1))
          assertEquals(second, Map("1" -> 2))
        }
      })
    }
  }

  test("[cats-effect] zero maximum weight returns loaded values and retains only zero-weight results") {
    val resource = SetCache.expireAfterWrite[String, String, Int](
      "test",
      Duration.ofMinutes(1),
      (_: Int) => 1,
      0L,
      cacheEmptyKeys = true
    )
    TestControl.executeEmbed(resource.use { cache =>
      for {
        first <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 1)))
        second <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 2)))
        _ <- cache.getOrUpdate("empty", Set("missing"))(_ => IO.pure(Map.empty))
        empty <- cache.getOrUpdate("empty", Set("missing"))(_ =>
          IO.raiseError(new AssertionError("zero-weight result lost"))
        )
      } yield {
        assertEquals(first, Map("1" -> 1))
        assertEquals(second, Map("1" -> 2))
        assert(empty.isEmpty)
      }
    })
  }

  test("[cats-effect] oversized loaded sets are returned but are not retained") {
    val resource = SetCache.expireAfterAccess[String, String, Int](
      "test",
      Duration.ofMinutes(1),
      None,
      (_: Int) => 1,
      1L,
      cacheEmptyKeys = false
    )
    TestControl.executeEmbed(resource.use { cache =>
      val values = Map("1" -> 1, "2" -> 2)
      for {
        calls <- Ref.of[IO, Int](0)
        first <- cache.getOrUpdate("test", Set("1", "2"))(_ => calls.update(_ + 1).as(values))
        second <- cache.getOrUpdate("test", Set("1", "2"))(_ => calls.update(_ + 1).as(values))
        count <- calls.get
      } yield {
        assertEquals(first, values)
        assertEquals(second, values)
        assertEquals(count, 2)
      }
    })
  }

  test("[cats-effect] access expiry is extended by cache hits") {
    val resource = SetCache.expireAfterAccess[String, String, Int](
      "test",
      Duration.ofSeconds(10),
      cacheEmptyKeys = false
    )
    TestControl.executeEmbed(resource.use { cache =>
      for {
        _ <- cache.put("test", Map("1" -> 1))
        _ <- IO.sleep(9.seconds)
        first <- cache.getOrUpdate("test", Set("1"))(_ => IO.raiseError(new AssertionError("premature expiry")))
        _ <- IO.sleep(9.seconds)
        second <- cache.getOrUpdate("test", Set("1"))(_ => IO.raiseError(new AssertionError("access not refreshed")))
        _ <- IO.sleep(11.seconds)
        third <- cache.getOrUpdate("test", Set("1"))(_ => IO.pure(Map("1" -> 3)))
      } yield {
        assertEquals(first, Map("1" -> 1))
        assertEquals(second, Map("1" -> 1))
        assertEquals(third, Map("1" -> 3))
      }
    })
  }
}
