# Cats Effect utilities

A standalone Cats Effect adaptation of seven owner-supplied utilities. The utility author
attribution supplied by the repository owner is **Bobylev (Бобылев)**.
Read [PROVENANCE.md](PROVENANCE.md), [MIGRATION.md](MIGRATION.md), and
[TESTING.md](TESTING.md) alongside the implementation.

The implementation is polymorphic in `F[_]` and uses Cats Effect type classes, `Resource`, and FS2. Typed business errors remain
explicit `Either` values; exceptions and cancellation retain their separate Cats Effect meanings.
The application chooses its effect and runtime, including transformer stacks such as `Kleisli`.
No ZIO runtime, Prelude, streams, or test dependency is required.

| Utility | Purpose | Construction |
| --- | --- | --- |
| [RateLimits](src/main/scala/com/parrot669/catsutils/RateLimits.scala) | Per-user/per-path token buckets and configuration reload | `Resource[F, RateLimits[F]]` |
| [HistoricalLimits](src/main/scala/com/parrot669/catsutils/HistoricalLimits.scala) | Apply a bucket only before an age threshold | `Resource[F, HistoricalLimits[F]]` |
| [Cache](src/main/scala/com/parrot669/catsutils/cache/Cache.scala) | Caffeine expiry, weight limits, and coordinated loads | `Resource[F, Cache[F, K, E, V]]` |
| [SetCache](src/main/scala/com/parrot669/catsutils/cache/SetCache.scala) | Load missing members of a requested set | `Resource[F, SetCache[F, K1, K2, V]]` |
| [RangeMap](src/main/scala/com/parrot669/catsutils/cache/ranged/RangeMap.scala) | Pure half-open interval storage and gaps | `RangeMap.empty[V]` |
| [RangeCache](src/main/scala/com/parrot669/catsutils/cache/ranged/RangeCache.scala) | Load, stream, and invalidate ordered time ranges | `Resource[F, RangeCache[F, K, V]]` |
| [CircuitBreakers](src/main/scala/com/parrot669/catsutils/CircuitBreakers.scala) | Armeria effect guards and resource guards | `F[CircuitBreakers[F]]` with an explicit registry |

## Build and verification

Use Java 21 from this directory:

```sh
sh run-sbt compile Test/compile test scalafmtCheckAll scalafmtSbtCheck
```

The runner selects Java 21 on macOS or uses `JAVA_HOME`/`java` elsewhere. It downloads the
pinned public sbt launcher and Maven Central dependencies, isolates caches under ignored
`.runtime`, and avoids global sbt plugins. Set `CATSUTILS_RUNTIME_DIR` to an absolute path
to reuse a local cache. Dependencies must already be cached when running without network access.
Scalafmt uses the PARROT 669 backend style, with internal imports grouped under
`com.parrot669.catsutils`. `scalafmtAll` formats Scala sources and README Scala examples;
`scalafmtSbt` formats build files. `scalafmtCheckAll` checks
those examples via task-scoped sources and `scala mdoc` fences; no mdoc runtime is required.
`publish / skip` is enabled; no Maven publication or deployment is configured.

Pinned versions: Scala 2.13.18, sbt 1.12.11, Cats Effect/testkit 3.7.1, Cats Core 2.13.0, Cats MTL 1.6.0,
FS2 3.12.2, Caffeine 3.2.4, Armeria 1.38.1, Micrometer 1.16.5, PureConfig 0.17.10,
Circe 0.14.10, Shapeless 2.3.13, munit 1.0.4, munit-cats-effect 2.0.0,
Scalafmt 3.8.1, sbt-scalafmt 2.5.6. These are this adaptation's build choices.
See [TESTING.md](TESTING.md) for reproducible checks and the test suite's scope.

## Usage

Allocate service resources once around the application lifetime. Allocating a cache on every
request creates an empty cache every time. Effectful dependencies are passed through parameters
or captured in loader functions.

```scala mdoc
import java.time.Duration

import cats.effect.Async

import com.parrot669.catsutils.cache.Cache

def example[F[_]: Async]: F[Either[String, Int]] =
  Cache.expireAfterWrite[F, String, String, Int]("answer", Duration.ofMinutes(5)).use { cache =>
    cache.getOrUpdate("key")(Async[F].pure(Right(42)))
  }
```

`Cache[F, K, E, V]` loaders return `F[Either[E, V]]` or `F[Either[E, Option[V]]]`.
A `Left` is cached, a raised exception is shared with current waiters but not cached, and `None`
is not retained. `get` returns only a successful cached value and hides loader/business failures;
canceling the caller of `get` still cancels that caller. For `Cache[F, K, Nothing, V]`, use the
convenience methods `getOrUpdateValue` and `getOrUpdateOptionValue` with ordinary effectful loaders.

SetCache and RangeCache provide ordinary effectful methods plus `getOrUpdateEither` and
`getOrUpdateOptionEither` for business errors. An empty SetCache request returns an empty map
without invoking the loader. `RangeCache.stream` returns `fs2.Stream[F, V]`;
`streamEither` returns `Stream[F, Either[E, V]]`, emits at most one `Left`, and then ends.
Earlier consumed gaps may already be cached. Raised exceptions still fail the stream.
Range fills run in owned child fibers; cleanup finishes before their key permit is released.
A fill that cancels itself cancels the requesting effect. See the migration notes for fiber-local context behavior.

CircuitBreakers accepts an explicit `PrometheusMeterRegistry` owned by the application.
Use `guardEither[E, A]()` for `F[Either[E, A]]` so `Left` counts as failure by default.
Plain `guard[A]()` classifies successful effect completion as success. Both allow an effectful
`Outcome` predicate. `scoped()` returns a `Resource[F, Unit]` and records the resource-use
`ExitCase`; wrap `resource.use(...)` with `guardEither` when a returned `Left` must count as failure.

## Effect requirements and migration

The effect parameter is first: `Cache[F, K, E, V]`, `SetCache[F, K1, K2, V]`,
`RangeCache[F, K, V]`, `RateLimits[F]`, `HistoricalLimits[F]`, and `CircuitBreakers[F]`.
Factories for caches, limiters, and circuit breakers require `Async[F]`: their implementations
combine suspended Java calls with effect clocks, references, cancellation, and concurrent work.
`TokenBucket.make[F]` only requires `Temporal[F]`; `TimeRange.makeF[F]` and the Micrometer
adapter require `Sync[F]`. `CacheMetrics.noop[F]` requires only `Applicative[F]`.
The context adapter needs `Monad[F]` and an explicit `Local[F, String]` carrier.
No library method executes an unsafe runtime or converts the caller's effect to IO.

This is a source-breaking change: add the effect argument to types and explicitly typed
factory calls, replace `TimeRange.makeIO(...)` with `TimeRange.makeF[F](...)`, and replace
`CacheMetricContext.create` with application-owned context allocation plus `fromLocal`.
Pure RangeMap, TimeRange values, and configuration types remain effect-free.

## Metrics and route context

Metrics are explicit and default to `CacheMetrics.noop`. Supply a shared adapter to any cache
or limiter constructor to enable the source metric names and labels. The adapter does not own
or close the registry. `CacheMetricContext.fromLocal[F]` accepts an application-owned
`cats.mtl.Local[F, String]`. Use a concurrency-safe carrier that preserves nested scopes and fiber isolation.
For an IO application, allocate `IOLocal` at the application boundary and pass its `asLocal` adapter:

```scala mdoc
import java.time.Duration

import cats.effect.{IO, IOLocal, Resource}

import com.parrot669.catsutils.cache.{Cache, CacheMetricContext, CacheMetrics}

import io.micrometer.core.instrument.simple.SimpleMeterRegistry

val services = for {
  registry <- Resource.make(IO(new SimpleMeterRegistry))(r => IO(r.close()))
  local    <- Resource.eval(IOLocal(""))
  context   = CacheMetricContext.fromLocal[IO](local.asLocal)
  metrics   = CacheMetrics.micrometer(registry, context)
  cache    <- Cache.expireAfterWrite[IO, String, Nothing, Int]("items", Duration.ofMinutes(5), metrics)
} yield (cache, context)

val request = services.use { case (cache, context) =>
  context.withPath("/items/:id")(cache.getOrUpdateValue("42")(IO.pure(42)))
}
```

`withPathFrom(resolveRoute)(effect)` resolves a path from the application effect. Prefer route templates
over unbounded raw URLs. Generic cache gauges retain `cache_hits_total`, `cache_misses_total`,
`cache_size`, `cache_weight`, and `cache_max_weight` with the `name` label; they record immediately
and every 30 seconds while the cache resource is alive. Weight gauges apply only to weighted caches.
Set/range counters retain `set_cache_hits_total`, `set_cache_loads_total`,
`range_cache_hits_total`, and `range_cache_loads_total`, with `name`/`path` and the hit's `locked` label.
Limiter cache names remain `Rate Limits` and `Legacy Rate Limits`. Adapters sharing one registry
share gauge backing for identical metric names and tags.

## Boundaries

Cancellation and locks are cooperative: a loader that deliberately masks cancellation can delay
its caller's cancellation and resource finalization. Single-flight coordination is per resident
cache generation; expiry, eviction, or explicit invalidation may permit a replacement loader.
An old generation never overwrites an explicit replacement. Zero retention can intentionally
produce uncached work and does not guarantee coalescing. Details and deliberate source fixes
are in [MIGRATION.md](MIGRATION.md).

The supplied reduced TimeRange and duration configuration behavior is retained, including
documented quirks. These tests establish the listed scenarios, not every possible scheduler
interleaving, input extreme, or application integration.

## License

This project uses the [Apache License 2.0](LICENSE), as selected by the repository owner.
Third-party terms and attribution remain applicable as described in
[PROVENANCE.md](PROVENANCE.md).
