package com.parrot669.catsutils

import java.time.{Duration, Instant}

import cats.data.Kleisli
import cats.effect.{Clock, Concurrent, IO, Ref, Resource, Sync, SyncIO, Temporal}
import cats.mtl.Local
import cats.syntax.all._
import cats.{Applicative, FlatMap, MonadThrow}

import com.parrot669.catsutils.cache.ranged.RangeCache
import com.parrot669.catsutils.cache.{Cache, CacheMetricContext, CacheMetrics, SetCache}
import com.parrot669.catsutils.domain.TimeRange

import fs2.{Compiler, Stream}
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}
import munit.CatsEffectSuite

class MinimalCapabilitiesTest extends CatsEffectSuite {
  private val ttl = Duration.ofMinutes(1)
  type App[A] = Kleisli[IO, String, A]

  // These generic entry points are compile-time regressions: no Async evidence is in scope.
  // Keep the README's generic Resource.use example compiled as well as formatted.
  private def example[F[_]: Sync: Temporal]: F[Either[String, Int]] =
    Cache
      .expireAfterWrite[F, String, String, Int]("answer", Duration.ofMinutes(5))
      .use { cache =>
        cache.getOrUpdate("key")(Temporal[F].pure(Right(42)))
      }(Temporal[F])

  private def raw[F[_]: Sync: Temporal]: Resource[F, Cache[F, String, Nothing, Int]] =
    Cache.expireAfterWrite[F, String, Nothing, Int]("minimal-raw", ttl)

  private def sets[F[_]: Sync: Temporal]: Resource[F, SetCache[F, String, String, Int]] =
    SetCache.expireAfterWrite[F, String, String, Int]("minimal-set", ttl, cacheEmptyKeys = true)

  private def ranges[F[_]: Sync: Temporal]: Resource[F, RangeCache[F, String, Int]] =
    RangeCache.make[F, String, Int]("minimal-range", RangeCache.Config.expireAfterWrite(ttl))

  private def rates[F[_]: Sync: Temporal](config: F[RateLimits.Config]): Resource[F, RateLimits[F]] =
    RateLimits.make[F](config)

  private def historical[F[_]: Sync: Temporal](config: F[HistoricalLimits.Config]): Resource[F, HistoricalLimits[F]] =
    HistoricalLimits.make[F](config)

  private def breakers[F[_]: Sync: Concurrent](
    config:   F[CircuitBreakers.Config],
    registry: PrometheusMeterRegistry
  ): F[CircuitBreakers[F]] =
    CircuitBreakers.make[F]("minimal-breaker", config, registry, shards = 1)

  private def bucket[F[_]: MonadThrow: Clock: Ref.Make]: F[RateLimits.TokenBucket[F]] =
    RateLimits.TokenBucket.make[F](1, ttl)

  private def context[F[_]](local: Local[F, String]): CacheMetricContext[F] = CacheMetricContext.fromLocal(local)

  private def resolvePath[F[_]: FlatMap](context: CacheMetricContext[F], path: F[String]): F[String] =
    context.withPathFrom(path)(context.path)

  private def noop[F[_]: Applicative]: CacheMetrics[F] = CacheMetrics.noop[F]

  test("all factories compile with their declared weaker capabilities and run with Kleisli") {
    val F         = Temporal[App]
    val resources = for {
      cache   <- raw[App]
      set     <- sets[App]
      range   <- ranges[App]
      rate    <- rates[App](F.pure(RateLimits.Config(RateLimits.RateLimitsConfig(1, ttl), Map.empty)))
      history <-
        historical[App](
          F.pure(HistoricalLimits.Config(HistoricalLimits.LegacyLimitsConfig(Duration.ZERO, 1, ttl), Map.empty))
        )
    } yield (cache, set, range, rate, history)
    resources
      .use { case (cache, set, range, rate, history) =>
        val interval = TimeRange.make(Instant.EPOCH, Instant.EPOCH.plusSeconds(1)).toOption.get
        for {
          result <- example[App]
          _      <- Sync[App].delay(assertEquals(result, Right(42)))
          a      <- cache.getOrUpdateValue("a")(F.pure(1))
          b      <- set.getOrUpdate("g", Set("b"))(_ => F.pure(Map("b" -> 2)))
          c      <- range.getOrUpdate("g", interval)(_ => F.pure(List(Instant.EPOCH -> 3)))
          d      <- rate("user", "path")
          e      <- history("user", "path", Instant.MIN)
          path   <- resolvePath(context[App](implicitly[Local[App, String]]), F.pure("inner"))
          _      <- noop[App].increment("ignored", Map.empty)
          _      <- Sync[App].delay {
                      assertEquals(a, 1); assertEquals(b, Map("b" -> 2)); assertEquals(c, List(3)); assert(d && e);
                      assertEquals(path, "inner")
                    }
        } yield ()
      }
      .run("outer")
  }

  test("circuit breaker construction needs no Temporal capability at its generic boundary") {
    Resource.make(IO(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))(r => IO(r.close())).use { registry =>
      val config = CircuitBreakers.Config(CircuitBreakers.CircuitBreakerConfig(0.5, 10, ttl, ttl), Map.empty)
      breakers[IO](IO.pure(config), registry)
        .flatMap(_.guard[Int]()("path")(IO.pure(42)))
        .map(result => assertEquals(result, 42))
    }
  }

  test("token bucket works with SyncIO, which has no Concurrent or Temporal instance") {
    val result = bucket[SyncIO].flatMap(b => (b.take, b.take).tupled).unsafeRunSync()
    assertEquals(result, (true, false))
  }

  test("RangeCache convenience API only needs Functor and an FS2 compiler") {
    def facade[F[_]: Applicative](implicit compiler: Compiler[F, F]): RangeCache[F, String, Int] =
      new RangeCache[F, String, Int] {
        def streamEither[E](key: String, range: TimeRange)(
          fill: TimeRange => F[Either[E, Option[Iterable[(Instant, Int)]]]]
        ): Stream[F, Either[E, Int]] =
          Stream.emit(Right(42): Either[E, Int]).covary[F]
        def put(key:        String, range: TimeRange, values: Iterable[(Instant, Int)]): F[Unit] = Applicative[F].unit
        def invalidate(key: String, from:  Instant): F[Unit] = Applicative[F].unit
        def keys: F[Set[String]] = Applicative[F].pure(Set.empty)
        def size: F[Long]        = Applicative[F].pure(0L)
        def weight: F[Long]      = Applicative[F].pure(0L)
      }
    val interval                                                                                 = TimeRange.make(Instant.EPOCH, Instant.EPOCH.plusSeconds(1)).toOption.get
    val values                                                                                   = facade[SyncIO].getOrUpdate("key", interval)(_ => SyncIO.pure(Nil)).unsafeRunSync()
    assertEquals(values, List(42))
  }
}
