package io.github.lelik112.catsutils.cache

import java.time.{Duration => JDuration}
import scala.concurrent.duration._

import cats.effect.{Deferred, IO}
import cats.effect.testkit.TestControl
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import munit.CatsEffectSuite

class CacheMetricContextTest extends CatsEffectSuite {
  test("paths are inherited by child fibers and nested paths are restored") {
    for {
      context <- CacheMetricContext.create
      before <- context.path
      values <- context.withPath("outer") {
        for {
          child <- context.path.start.flatMap(_.joinWithNever)
          inner <- context.withPath("inner")(context.path)
          restored <- context.path
        } yield (child, inner, restored)
      }
      after <- context.path
    } yield { assertEquals(before, ""); assertEquals(values, ("outer", "inner", "outer")); assertEquals(after, "") }
  }

  test("an application can resolve a path from its own context") {
    for {
      context <- CacheMetricContext.create
      actual <- context.withPathFrom(IO.pure("/items/:id"))(context.path)
      after <- context.path
    } yield { assertEquals(actual, "/items/:id"); assertEquals(after, "") }
  }

  test("cache counters retain name, locked and resolved path tags") {
    IO(new SimpleMeterRegistry).bracket { registry =>
      for {
        context <- CacheMetricContext.create
        metrics = SpecificCacheMetrics("context_probe", "context-test", CacheMetrics.micrometer(registry, context))
        _ <- context.withPath("/items/:id")(metrics.load *> metrics.hit(locked = true))
        _ <- IO {
          assertEquals(
            registry
              .get("context_probe_loads_total")
              .tags("name", "context-test", "path", "/items/:id")
              .counter()
              .count(),
            1.0
          )
          assertEquals(
            registry
              .get("context_probe_hits_total")
              .tags("name", "context-test", "path", "/items/:id", "locked", "true")
              .counter()
              .count(),
            1.0
          )
        }
      } yield ()
    }(r => IO(r.close()))
  }

  test("path scope restores state after a raised exception") {
    for {
      context <- CacheMetricContext.create
      _ <- context.withPath("broken")(IO.raiseError[Unit](new RuntimeException("fail"))).attempt
      after <- context.path
    } yield assertEquals(after, "")
  }

  test("path scope restores state before cancellation finalizers") {
    for {
      context <- CacheMetricContext.create
      entered <- Deferred[IO, Unit]
      observed <- Deferred[IO, String]
      fiber <- context
        .withPath("inside")(entered.complete(()) *> IO.never)
        .guarantee(context.path.flatMap(observed.complete).void)
        .start
      _ <- entered.get *> fiber.cancel
      restored <- observed.get
    } yield assertEquals(restored, "")
  }

  test("gauge names, values, interval and resource-owned meter lifetime") {
    TestControl.executeEmbed(IO(new SimpleMeterRegistry).bracket { registry =>
      for {
        context <- CacheMetricContext.create
        metrics = CacheMetrics.micrometer(registry, context)
        allocated <- Cache
          .expireAfterWrite[Int, Nothing, String](
            "metered",
            JDuration.ofDays(1),
            10L,
            (value: String) => value.length,
            metrics
          )
          .allocated
        (cache, release) = allocated
        _ <- (for {
          _ <- cache.put(1, "abc") *> cache.get(1) *> cache.get(2)
          _ <- IO.sleep(31.seconds)
          _ <- IO {
            def gauge(name: String): Double = registry.get(name).tag("name", "metered").gauge().value()
            assertEquals(gauge("cache_hits_total"), 1.0)
            assertEquals(gauge("cache_misses_total"), 1.0)
            assertEquals(gauge("cache_size"), 1.0)
            assertEquals(gauge("cache_weight"), 3.0)
            assertEquals(gauge("cache_max_weight"), 10.0)
          }
        } yield ()).guarantee(release)
        _ <- cache.put(2, "d") *> IO.sleep(31.seconds)
        _ <- IO(assertEquals(registry.get("cache_size").tag("name", "metered").gauge().value(), 1.0))
      } yield ()
    }(r => IO(r.close())))
  }

  test("adapters sharing a registry update the same labeled gauge") {
    IO(new SimpleMeterRegistry).bracket { registry =>
      for {
        firstContext <- CacheMetricContext.create
        secondContext <- CacheMetricContext.create
        first = CacheMetrics.micrometer(registry, firstContext)
        second = CacheMetrics.micrometer(registry, secondContext)
        _ <- first.gauge("shared", Map("name" -> "same"), 1.0)
        _ <- second.gauge("shared", Map("name" -> "same"), 2.0)
        _ <- IO(assertEquals(registry.get("shared").tag("name", "same").gauge().value(), 2.0))
        _ <- first.gauge("shared", Map("name" -> "same"), 3.0)
        _ <- IO(assertEquals(registry.get("shared").tag("name", "same").gauge().value(), 3.0))
      } yield ()
    }(r => IO(r.close()))
  }
}
