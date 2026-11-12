package com.parrot669.catsutils

import java.time.{Duration, Instant}
import scala.concurrent.duration._

import cats.data.Kleisli
import cats.effect.syntax.all._
import cats.effect.testkit.TestControl
import cats.effect.{Async, Deferred, IO, Resource}
import cats.mtl.Local
import cats.syntax.all._

import com.parrot669.catsutils.cache.ranged.RangeCache
import com.parrot669.catsutils.cache.{Cache, CacheMetricContext, CacheMetrics, SetCache}
import com.parrot669.catsutils.domain.TimeRange

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}
import munit.CatsEffectSuite

class EffectPolymorphismTest extends CatsEffectSuite {
  type App[A] = Kleisli[IO, String, A]
  private val F     = Async[App]
  private val ttl   = Duration.ofMinutes(5)
  private val from  = Instant.EPOCH
  private val until = from.plusSeconds(2)

  test("Cache and SetCache execute loaders in the caller's Kleisli environment") {
    val program = for {
      raw <- Cache.expireAfterWrite[App, String, String, String]("generic-raw", ttl)
      set <- SetCache.expireAfterWrite[App, String, Int, String]("generic-set", ttl, cacheEmptyKeys = true)
    } yield (raw, set)
    program
      .use { case (raw, set) =>
        for {
          first  <- raw.getOrUpdate("key")(Kleisli.ask[IO, String].map(Right(_)))
          cached <- raw.getOrUpdate("key")(F.raiseError(new AssertionError("cache hit evaluated loader")))
          values <-
            set.getOrUpdate("bucket", Set(1, 2))(keys => Kleisli.ask[IO, String].map(env => keys.map(_ -> env).toMap))
          _      <- F.delay {
                      assertEquals(first, Right("request")); assertEquals(cached, first);
                      assertEquals(values, Map(1 -> "request", 2 -> "request"))
                    }
        } yield ()
      }
      .run("request")
  }

  test("RangeCache streams ordered values with a Kleisli loader") {
    val range  = TimeRange.make(from, until).toOption.get
    val config = RangeCache.Config.expireAfterWrite(ttl).withMaxGap(Duration.ofSeconds(1)).withParallelism(2)
    RangeCache
      .make[App, String, String]("generic-range", config)
      .use { cache =>
        cache
          .stream("key", range)(gap =>
            Kleisli.ask[IO, String].map(env => Some(List(gap.start -> s"$env:${gap.start.getEpochSecond}")))
          )
          .compile
          .toList
          .flatMap(values => F.delay(assertEquals(values, List("request:0", "request:1"))))
      }
      .run("request")
  }

  test("limiter constructors, token buckets and TimeRange validation are generic") {
    val rate       = RateLimits.Config(RateLimits.RateLimitsConfig(1, ttl), Map.empty)
    val historical = HistoricalLimits.Config(HistoricalLimits.LegacyLimitsConfig(Duration.ZERO, 1, ttl), Map.empty)
    val services   = (RateLimits.make[App](F.pure(rate)), HistoricalLimits.make[App](F.pure(historical))).tupled
    services
      .use { case (rates, history) =>
        for {
          bucket  <- RateLimits.TokenBucket.make[App](1, ttl)
          takes   <- (bucket.take, bucket.take).tupled
          allowed <- (rates("user", "path"), rates("user", "path")).tupled
          old     <- (history("user", "path", Instant.MIN), history("user", "path", Instant.MIN)).tupled
          range   <- TimeRange.makeF[App](from, until)
          invalid <- TimeRange.makeF[App](until, from).attempt
          _       <- F.delay {
                       assertEquals(takes, (true, false)); assertEquals(allowed, (true, false));
                       assertEquals(old, (true, false)); assertEquals(range.start, from); assert(invalid.isLeft)
                     }
        } yield ()
      }
      .run("request")
  }

  test("circuit breaker outcomes, fallback signatures and scoped resources retain F") {
    Resource.make(IO(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))(r => IO(r.close())).use { registry =>
      val settings = CircuitBreakers.CircuitBreakerConfig(0.5, 10L, ttl, ttl)
      (for {
        breaker <-
          CircuitBreakers
            .make[App]("generic-breaker", F.pure(CircuitBreakers.Config(settings, Map.empty)), registry, shards = 1)
        value   <- breaker.guard[String]()("path")(Kleisli.ask[IO, String])
        typed   <- breaker.guardEither[String, String]()("path")(F.pure(Left("business")))
        scoped  <- breaker.scoped()("scoped").use(_ => Kleisli.ask[IO, String])
        _       <- F.delay {
                     assertEquals(value, "request"); assertEquals(typed, Left("business")); assertEquals(scoped, "request")
                   }
      } yield ()).run("request")
    }
  }

  test("generic Local context isolates siblings and restores nesting, failures and cancellation") {
    val context = CacheMetricContext.fromLocal[App](implicitly[Local[App, String]])
    (for {
      nested       <- context.withPath("outer")((context.withPath("inner")(context.path), context.path).tupled)
      siblings     <-
        (context.withPath("left")(F.cede *> context.path), context.withPath("right")(F.cede *> context.path)).parTupled
      _            <- context.withPath("failed")(F.raiseError[Unit](new RuntimeException("expected"))).attempt
      entered      <- Deferred[App, Unit]
      restored     <- Deferred[App, String]
      fiber        <- context
                        .withPath("canceled")(entered.complete(()) *> F.never[Unit])
                        .guarantee(context.path.flatMap(restored.complete).void)
                        .start
      _            <- entered.get *> fiber.cancel
      canceledPath <- restored.get
      after        <- context.path
      _            <- F.delay {
                        assertEquals(nested, ("inner", "outer")); assertEquals(siblings, ("left", "right"));
                        assertEquals(canceledPath, "root"); assertEquals(after, "root")
                      }
    } yield ()).run("root")
  }

  test("a canceled generic cache loader releases a waiter to retry") {
    Cache
      .expireAfterWrite[App, String, Nothing, String]("generic-cancel", ttl)
      .use { cache =>
        for {
          entered <- Deferred[App, Unit]
          owner   <- cache.getOrUpdateValue("key")(entered.complete(()) *> F.never[String]).start
          _       <- entered.get
          waiter  <- cache.getOrUpdateValue("key")(Kleisli.ask[IO, String]).start
          _       <- F.cede *> owner.cancel
          result  <- waiter.joinWithNever
          cached  <- cache.get("key")
          _       <- F.delay { assertEquals(result, "retry"); assertEquals(cached, Some("retry")) }
        } yield ()
      }
      .run("retry")
      .timeout(5.seconds)
  }

  test("parallel generic RangeCache self-cancellation cancels the consumer") {
    val range  = TimeRange.make(from, until).toOption.get
    val config = RangeCache.Config.expireAfterWrite(ttl).withMaxGap(Duration.ofSeconds(1)).withParallelism(2)
    RangeCache
      .make[App, String, Int]("generic-self-cancel", config)
      .use { cache =>
        for {
          worker  <- cache.getOrUpdate("key", range)(_ => F.canceled *> F.never[Iterable[(Instant, Int)]]).start
          outcome <- worker.join
          values  <- cache.getOrUpdate("key", range)(gap => F.pure(List(gap.start -> 1)))
          _       <- F.delay { assert(outcome.isCanceled); assertEquals(values, List(1, 1)) }
        } yield ()
      }
      .run("request")
      .timeout(5.seconds)
  }

  test("generic metrics preserve path and their background fiber stops at Resource release") {
    TestControl.executeEmbed(Resource.make(IO(new SimpleMeterRegistry))(r => IO(r.close())).use { registry =>
      val context = CacheMetricContext.fromLocal[App](implicitly[Local[App, String]])
      val metrics = CacheMetrics.micrometer[App](registry, context)
      (for {
        allocated       <- Cache.expireAfterWrite[App, Int, Nothing, Int]("generic-meter", ttl, metrics).allocated
        (cache, release) = allocated
        _               <- (cache.put(1, 1) *> F.sleep(31.seconds) *> F.delay(
                             assertEquals(registry.get("cache_size").tag("name", "generic-meter").gauge().value(), 1.0)
                           )).guarantee(release)
        _               <- cache.put(2, 2) *> F.sleep(31.seconds)
        _               <- context.withPath("/generic")(metrics.increment("generic_requests", Map.empty))
        path            <- context.withPath("/generic")(metrics.path)
        _               <- F.delay {
                             assertEquals(registry.get("cache_size").tag("name", "generic-meter").gauge().value(), 1.0);
                             assertEquals(path, "/generic")
                           }
      } yield ()).run("root")
    })
  }
  test("generic convenience loaders preserve failure for null results rather than treating them as absent") {
    val range    = TimeRange.make(from, until).toOption.get
    val services = (
      SetCache.expireAfterWrite[App, String, Int, String]("null-set", ttl, cacheEmptyKeys = true),
      RangeCache.make[App, String, String]("null-range", RangeCache.Config.expireAfterWrite(ttl))
    ).tupled
    services
      .use { case (set, ranged) =>
        for {
          setPlain   <- set.getOrUpdate("plain", Set(1))(_ => F.pure(null: Map[Int, String])).attempt
          setTyped   <- set.getOrUpdateEither[String]("typed", Set(1))(_ => F.pure(Right(null: Map[Int, String]))).attempt
          rangePlain <- ranged.getOrUpdate("plain", range)(_ => F.pure(null: Iterable[(Instant, String)])).attempt
          rangeTyped <-
            ranged
              .getOrUpdateEither[String]("typed", range)(_ => F.pure(Right(null: Iterable[(Instant, String)])))
              .attempt
          _          <- F.delay {
                          assert(setPlain.isLeft); assert(setTyped.isLeft); assert(rangePlain.isLeft); assert(rangeTyped.isLeft)
                        }
        } yield ()
      }
      .run("request")
  }
}
