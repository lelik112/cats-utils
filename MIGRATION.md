# Migration and behavioral decisions

This document distinguishes contracts retained from the owner-supplied reference from intentional
adaptation choices and edge cases. See [PROVENANCE.md](PROVENANCE.md) for attribution and
[TESTING.md](TESTING.md) for reproducible checks.

## Namespace

Library and test sources use `com.parrot669.catsutils`, matching the PARROT 669 namespace.
Update consuming imports and fully qualified type references accordingly. The build
organization is `com.parrot669` and the project name remains `cats-utils`; Maven publication
is still disabled. This namespace change retains the utility contracts described below.

## Migrating the initial IO API to F

Effect-bearing types and constructors now take `F[_]` as the first type parameter.
Use `Cache[F, K, E, V]` / `Cache.UCache[F, K, V]`, `SetCache[F, K1, K2, V]`,
`RangeCache[F, K, V]`, `RateLimits[F]`, `RateLimits.TokenBucket[F]`,
`HistoricalLimits[F]`, and `CircuitBreakers[F]` / `CircuitBreakers.Guarded[F, A]`.
Metrics use `CacheMetrics[F]` and `CacheMetricContext[F]` throughout. `Outcome`, `Resource`,
FS2 streams, loaders, fallbacks, configuration reads, and callbacks all retain the same `F`.

`TimeRange.makeIO` is replaced by `TimeRange.makeF[F: Sync]` (both overloads). Validation
still runs only when the effect runs. `CacheMetricContext.create` is replaced by
`CacheMetricContext.fromLocal[F](local)`: the application supplies a concurrency-safe Cats MTL Local.
For IO, allocate `IOLocal("")` in the application and pass `.asLocal`. For `Kleisli`, use its
Local instance. A shared mutable `Ref` is not a replacement for fiber-local context.

Cache and limiter factories take separate `Sync[F]` and `Temporal[F]` evidence. Circuit-breaker
factories take `Sync[F]` and `Concurrent[F]`. No `Async[F]` instance is required. If an application
passes implicit arguments explicitly, update that argument list as well: for example, RangeCache
construction now takes `Sync[F]`, `Temporal[F]`, then the instant ordering. Normally these are
inferred. See the README's effect requirements for the weaker supporting and abstract APIs.

This migration deliberately changes source signatures, not cache retention, typed errors,
configuration ordering, cancellation ownership, or limiter/circuit-breaker policy.
The original IO tests remain runtime-bound tests; transformer tests verify that the public API
and implementations also run without IO fixed inside the library.

## Effect and resource APIs

| Source concept | Cats Effect API |
| --- | --- |
| `ZIO[R, E, A]` environment and typed failure | Explicit parameters/closures plus `F[Either[E, A]]` where typed failure matters |
| `UIO[A]` and defects | `F[A]`; a raised Throwable remains an effect failure, not a cached business error |
| `Scope` and `forkScoped` | `Resource[F, A]` and a resource-owned metric fiber |
| `Promise` carrying an exit | `Deferred` carrying completed business result, exception, or retry signal |
| ZIO `Exit` success predicate | An effectful Cats Effect `Outcome` predicate |
| Scoped exit predicate | An effectful `Resource.ExitCase` predicate |
| `ZStream` | FS2 `Stream[F, A]`, or `Stream[F, Either[E, A]]` for typed business failures |
| ZIO nonempty set | Scala `Set`; an empty request returns empty without a load |
| Fiber-local log annotation | `CacheMetricContext[F]` backed by application-supplied `cats.mtl.Local[F, String]` |
| ZIO Duration | `java.time.Duration`; Effect sleeps and IO test runners use Scala finite durations |

This module is polymorphic in `F[_]`; effectful constructors use Cats Effect constraints. It avoids unsafe runtimes and global effect execution to bridge
synchronous Java callbacks. Allocation and operations remain lazy effects; by-name raw-cache
loaders are not evaluated on hits. Public resource handles should be used within their allocation
scope. Closing a cache resource stops its meter; it does not cancel unrelated request fibers that
the caller started and still owns.

## Cache

Preserved behavior:

- Successful values and `Left` business errors are cached. A raised exception, an interrupted
  loader, and a successful `None` are not retained as completed values.
- Current waiters share their generation's result. Canceling a waiter does not cancel its loader.
  Canceling the loader releases waiters, which retry using their own loader; raw `get` returns None.
- `getOrUpdateValue` retries if it joined an optional loader that returned None. Business-error
  caches use explicit `getOrUpdate`/`getOrUpdateOption`; `get` hides failures as None.
- Caffeine continues to control write/access expiry, admission and weighted eviction. The optional
  `refresh` is a maximum lifetime since write, not a background refresh. A new load's write lifetime
  begins at completion even when its placeholder expired before completion.
- Loading entries and cached errors weigh one. Successful weighted entries use the supplied weigher;
  unweighted `weight` is zero. Expiration/eviction can remove a loading placeholder and allow another
  loader, so coalescing is per resident generation, not an unconditional per-key global mutex.

Intentional corrections:

- The source raw Cache unconditionally published late loads. This port tracks active generations:
  explicit invalidate/put and replacement loads retire an older generation, which cannot overwrite
  or delete the new state. Existing waiters still receive their own original result. No loader is
  canceled merely because the key was invalidated.
- User weight computation occurs once before mutation and must return a nonnegative Int. If it
  throws, current waiters fail and later callers can retry; a failed explicit put leaves an active
  loader intact. `maxWeight` and `weigher` must be supplied together.
- Canceling `get` remains cancellation rather than being swallowed as a cache miss. There is no
  ZIO composite Cause tree; exceptions use the ordinary effect error channel.
- A monotonic effect clock updates Caffeine's ticker using an atomic maximum, so TestControl and
  concurrent clock reads work without `unsafeRunSync`. Caffeine maintenance executes directly.
  Expired values are enforced on access/explicit maintenance and the 30-second metric loop rather
  than a separate Java scheduler. `keys`, `size`, and `weight` perform cleanup before reporting.

## SetCache and RangeCache

Per-key permits cover loads and updates; cancellation, failures, and stream termination release
them. Invalidation coordinates with commits so obsolete loaders cannot recreate invalidated entries.
If admission (or access-mode group expiry) removes an entry before its lock can be used, a detached empty entry serves
that request and is not published. This fixes the source's potential endless lock-identity retry
with zero retention or immediate eviction; concurrent uncached requests need not coalesce.

SetCache preserves requested-subset filtering and missing-key caching. `Some(Map.empty)` can mark
requested keys absent when `cacheEmptyKeys` is enabled; `None` means do not cache those missing keys.
Business failures and raised exceptions leave existing state intact. Extra keys returned by a loader
may be retained as in the source but are not returned unless requested.

Stage B deliberately changes SetCache factories and storage semantics:

- Previously unbounded factories now require `maxWeight` immediately before `cacheEmptyKeys`.
  Weighted signatures retain `size, maxWeight, cacheEmptyKeys`. Metrics remain the final argument.
  All factories keep `Sync[F]` + `Temporal[F]`; the abstract API still needs only `Functor[F]`.
- Every retained K2 has independent write/access timestamps. Successful fill/put ages only new or
  overwritten values. An extra returned key is a write too. Empty put is a no-op. Positive and
  negative entries use the same policy, and equality at an expiry boundary is a miss.
- Access hits now obtain the group permit and may wait for a fill. They touch only requested live
  cells at selection time. A successful slow response can include earlier selected hits; retention
  prunes expired cells at publication, and a failed/canceled operation publishes no touch.
  Optional `refresh` is each cell's maximum write age. It is not background refresh.
- Write-only cell freshness is separate from physical group lifetime: no outer write timer detaches
  a group during a long fill. New cells begin their write age at successful publication even when
  the load exceeds TTL. Idle expired cells remain charged until a later publication prunes them or
  capacity evicts the group; freshness never returns them as new hits. Same-group misses wait for
  the active loader rather than creating a timer replacement. Access/refresh group timers are unchanged.
  Explicit invalidate and capacity eviction/replacement still prohibit late publication. Different-group
  loaders remain parallel, invalidate does not wait for user code, and creation/final publication/
  oversized removal retain the same identity gate. This changes cleanup timing and scheduling.
- Negative entries and present zero-size values now cost one unit instead of zero. Unit-cost factories
  charge one per cell. Empty resident groups and placeholders also cost one. The shared budget bounds
  charged retained state with whole-K1 eviction, not per-ID LRU and not instantaneous total JVM memory.
  Temporary results, waiting callers and detached generations are outside it.
- `maxWeight=0` retains nothing, replacing the former zero-weight negative-retention behavior.
  Negative capacity/durations and durations not representable in nanoseconds fail effectful allocation.
  There is no hidden `Long.MaxValue` default or provider-specific capacity choice.

New positive values are weighed once before their publication; touches reuse stored charges. Every
member is validated even after the aggregate exceeds Int.MaxValue. Negative/throwing weighers leave
previous valid state intact. Sums use checked Long arithmetic. Caffeine requires an Int entry weight:
a larger aggregate is returned to the caller without retention even with a larger Long budget.
An oversized merged put removes the whole group. The generation check and removal remain under one
gate so a stale oversized fill cannot delete a replacement. No second capacity registry is added.

Result/error distinctions remain: empty requests do nothing, `None` does not mark missing keys absent,
`Some(empty)` can, and partial successful Maps are authoritative when `cacheEmptyKeys=true`. Consumers
must represent uncertain provider results as failure rather than absent members of a successful Map.
Left, exception and cancellation never become successful empty or negative entries. Unlike raw Cache,
SetCache waiters recheck under the permit and may perform their own fill after an error or unretained
result; they are not promised their predecessor's error. No retries, backoff or HTTP policy is added.

RangeMap keeps its DIET interval algorithm, half-open boundaries and configured instant ordering.
Ascending/reversed value and gap iteration is tested explicitly; adjacent ranges and duplicates
retain the source behavior. TimeRange union encloses the gap between disjoint intervals.

RangeCache preserves the following distinctions:

- Cached intervals can contain no values and still cover a range. A loader's `Some(empty)` marks
  coverage; `None` leaves a gap to be retried.
- Range invalidation marks the earliest stale instant. Existing callers can receive work begun
  before invalidation; subsequently cached data is cleaned against that cutoff. This port keeps
  the cutoff attached to every partial commit of the active load, preventing a later split gap
  from resurrecting stale data. A new lock owner cleans the stale state and starts a fresh generation.
- Gap merging happens before splitting. `ignoreRanges` can merge gaps across a small cached interval;
  the resulting load replaces that interval. `maxGap` splits from the lower bound, and emission
  follows the requested ascending or reversed ordering.
- `skipLast` filters entire gaps by whether their **start** is strictly before `now - skipLast`.
  It does not truncate an eligible gap at that threshold. `trimToNow` independently truncates
  the requested end; a wholly future request becomes empty.
- Successful parallel loads emit in order. Any raised or typed failure cancels outstanding work
  promptly, even if an earlier ordered gap is stalled. `streamEither` reifies the typed failure
  as one terminal Left after resource cleanup. The first competing failure wins; parallel execution
  does not promise which concurrent failure will be observed or which unfinished values will emit.
- Actual fills execute in supervised child fibers, including in the sequential path. The supervisor
  is scoped inside the per-key permit: child cancellation and finalizers complete before the permit
  is released or a terminal result is emitted. A fill that cancels itself cancels the requesting effect;
  it does not become a successful empty stream or a business Left. Child fibers use the application's context carrier. With the IO application adapter, IOLocal
  changes in child fibers do not merge into the caller's fiber. This explicit ownership
  is a deliberate lifecycle change needed to distinguish self-cancellation from consumer termination.
- Only consumed successful gap results are committed. Taking an early prefix or cancellation closes
  the permit and cancels outstanding loads; already committed gaps remain reusable.

Configuration now rejects nonpositive parallelism/maxGap and negative skipLast/ignoreRanges/maxWeight.
Gap splitting is iterative to avoid recursive overflow. The current clock is read again after a
lock wait. These are deliberate corrections to invalid-configuration/prolonged-wait edge cases.

## Limits and circuit breakers

RateLimits now requires an explicit positive `maxKeys` in `make(config, maxKeys)` and returns
`Allowed`, `Exhausted(retryAfter)`, or `CapacityRejected`. Convert with `decision.isAllowed` only
when the distinction is intentionally irrelevant. The former optional CacheMetrics argument is
removed: this implementation has no cache/meter or background task. Observe decisions at the caller.
The bounded map never evicts outstanding debt for capacity. Fully recovered entries retire lazily
when a new key needs a slot; expired-but-idle entries need no timer and remain bounded by `maxKeys`.
Closing the Resource clears storage; an in-flight read cannot resurrect it. Retained handles cannot
admit work after release. Capacity is fixed for that Resource; no implicit production limit is supplied.

Capacity, path/user overrides and per-key isolation remain. `per` refills **one token**, not a full
burst. Integer nanosecond debt retains fractional credit exactly. Both refill and capacity times
refill must fit positive signed nanoseconds; zero/negative and overflowing rules fail before use.
Elapsed arithmetic is exact across signed timestamp origins; a backwards observation grants no
credit. `Exhausted.retryAfterSeconds` rounds up for Retry-After; a competing request may consume
the next token, so the delay is not a reservation. Capacity rejection has no promised retry time.

Every successfully read configuration snapshot participates in observation ordering, even if invalid.
The latest accepted full snapshot is validated inside the critical section before bucket mutation.
An invalid snapshot suppresses older observations and raises an error without changing bucket debt;
failed source reads and canceled reads are not accepted. An effective valid rule change
explicitly resets that key to a full bucket in its existing slot; it is not capacity eviction.
Unchanged effective rules retain debt. A restart/new Resource resets all state. These are local
process guarantees, not durable or cross-instance quotas. Key count does not bound arbitrary key
string sizes or caller-supplied configuration size: use canonical bounded identifiers/logical paths.

HistoricalLimits still consumes a token only when the start instant is **strictly before** the age
threshold and retains its one-minute access-expiring cache. Its Boolean `TokenBucket.take` interface
is preserved; it shares the stricter positive/representable refill validation and exact debt arithmetic.
HistoricalLimits is not the bounded incoming-budget API and is otherwise outside this change.

Concurrent configuration reads receive increasing tickets immediately before evaluating the supplied
effect. Reads run outside the limiter mutex. After a read succeeds, the latest accepted snapshot is the
one with the greatest read-start ticket; an older completing read uses that accepted full snapshot.
Accepting a snapshot, selecting/replacing its effective bucket and consuming a token share one short
critical section per limiter instance. Equal snapshots also advance the accepted ticket, so a delayed
read cannot reset an exhausted bucket after a same-value read or an A-to-B-to-A reload. Historical
age checks use the same accepted snapshot. Failed/canceled reads and canceled mutex waiters publish
nothing; cancellation after entering the masked transaction may still consume a token.

This corrects the source's invalidate-and-retry race. The existing F[Config] API carries no external
revision: tickets order read initiation, not actual revisions in a remote configuration service. An
older-started read returning genuinely newer external data can therefore be superseded by a
later-started accepted read. Callers needing external revision ordering must provide an appropriately
consistent configuration source; the limiter cannot infer revisions from configuration values.

CircuitBreakers retains Armeria configuration, path isolation, reload, half-open/open windows,
fallback handlers and custom outcome classification. `guardEither` treats Left as failure by default;
plain `guard` sees a normally returned Left as successful effect completion. `Resource.ExitCase` does not
carry a return value, so `scoped` cannot infer a business Left from the use result. Wrap the complete
`resource.use` in `guardEither` when that classification is required. Inner finalizer failures are
included when the guard wraps the full use; the scoped observer sees the exit supplied by Resource.

Admission and installation of outcome accounting are cancellation-safe. Breaker construction is
serialized per shard outside retryable Ref transforms so listeners are not installed multiple times.
Shard counts must be positive and negative hash codes are mapped with `floorMod`. Registry lifecycle
belongs to the caller. A custom outcome predicate that itself raises an exception has normal Cats
Effect finalizer semantics; predicates should be total for reliable breaker accounting.

## Supporting types and instrumentation

TimeRange retains zoned construction validation, instant equality/order, half-open membership,
`from`/`to` codecs, and the enclosing union. `smallest` adds exactly 1000 nanoseconds despite the
retained source comment mentioning milliseconds. `makeF[F]` replaces the original `makeUIO` and raises
RuntimeException lazily for validation failures. Java temporal conversion/overflow errors remain
possible for unsupported extremes; the reduced source's omitted integrations remain omitted.

The duration reader preserves finite durations, including negative values, and maps every nonfinite
Scala duration to `Duration.ofNanos(Long.MaxValue)` as the source did. This sentinel is not mathematical
infinity. Utility-specific validation still applies afterward.

Global ZIO metrics become an explicit `CacheMetrics[F]` adapter, disabled by default. Metric names,
labels, limiter cache names and the 30-second resource lifetime are retained. Adapter instances
sharing a registry share backing for the same labeled gauge. Application code owns registry creation,
exporter selection and shutdown; no exporter or server is automatically started.
