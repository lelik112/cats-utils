# Migration and behavioral decisions

This document distinguishes contracts retained from the owner-supplied reference from intentional
adaptation choices and edge cases. See [PROVENANCE.md](PROVENANCE.md) for attribution and
[TESTING.md](TESTING.md) for reproducible checks.

## Namespace

Library and test sources use `com.parrot669.catsutils`, matching the PARROT 669 namespace.
Update consuming imports and fully qualified type references accordingly. The build
organization is `com.parrot669` and the project name remains `cats-utils`; Maven publication
is still disabled. This namespace change retains the utility contracts described below.

## Effect and resource APIs

| Source concept | Cats Effect API |
| --- | --- |
| `ZIO[R, E, A]` environment and typed failure | Explicit parameters/closures plus `IO[Either[E, A]]` where typed failure matters |
| `UIO[A]` and defects | `IO[A]`; a raised Throwable remains an effect failure, not a cached business error |
| `Scope` and `forkScoped` | `Resource[IO, A]` and a resource-owned metric fiber |
| `Promise` carrying an exit | `Deferred` carrying completed business result, exception, or retry signal |
| ZIO `Exit` success predicate | An effectful Cats Effect `Outcome` predicate |
| Scoped exit predicate | An effectful `Resource.ExitCase` predicate |
| `ZStream` | FS2 `Stream[IO, A]`, or `Stream[IO, Either[E, A]]` for typed business failures |
| ZIO nonempty set | Scala `Set`; an empty request returns empty without a load |
| Fiber-local log annotation | Explicitly allocated `CacheMetricContext` backed by `IOLocal` |
| ZIO Duration | `java.time.Duration`; IO sleeps/tests use Scala finite durations |

This module is IO-specific. It avoids unsafe runtimes and global effect execution to bridge
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
  ZIO composite Cause tree; exceptions use the ordinary IO error channel.
- A monotonic effect clock updates Caffeine's ticker using an atomic maximum, so TestControl and
  concurrent clock reads work without `unsafeRunSync`. Caffeine maintenance executes directly.
  Expired values are enforced on access/explicit maintenance and the 30-second metric loop rather
  than a separate Java scheduler. `keys`, `size`, and `weight` perform cleanup before reporting.

## SetCache and RangeCache

Per-key permits cover loads and updates; cancellation, failures, and stream termination release
them. Invalidation coordinates with commits so obsolete loaders cannot recreate invalidated entries.
If expiry or admission removes an entry before its lock can be used, a detached empty entry serves
that request and is not published. This fixes the source's potential endless lock-identity retry
with zero retention or immediate eviction; concurrent uncached requests need not coalesce.

SetCache preserves requested-subset filtering and missing-key caching. `Some(Map.empty)` can mark
requested keys absent when `cacheEmptyKeys` is enabled; `None` means do not cache those missing keys.
Business failures and raised exceptions leave existing state intact. Extra keys returned by a loader
may be retained as in the source but are not returned unless requested.

Weighted SetCache entries validate every individual weight as nonnegative and sum with checked
Long arithmetic. Caffeine requires an Int per-entry weight: an aggregate greater than Int.MaxValue
is returned successfully to the current caller but is not retained, even if the configured total
maxWeight is larger. An oversized merged put removes the entire aggregate. Negative or throwing
weighers fail the operation and preserve the previous valid state. Each value is weighed once per
publication; Caffeine reads the precomputed weight. Generation creation shares the commit gate so
an old oversized result cannot invalidate a replacement created during its weight calculation.
This deliberately fixes the source's unchecked Int sum, which could wrap a positive weight to zero.

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
  is released or a terminal result is emitted. A fill that cancels itself cancels the requesting IO;
  it does not become a successful empty stream or a business Left. Child fibers inherit IOLocal
  context, but their local mutations do not merge into the caller's fiber. This explicit ownership
  is a deliberate lifecycle change needed to distinguish self-cancellation from consumer termination.
- Only consumed successful gap results are committed. Taking an early prefix or cancellation closes
  the permit and cancels outstanding loads; already committed gaps remain reusable.

Configuration now rejects nonpositive parallelism/maxGap and negative skipLast/ignoreRanges/maxWeight.
Gap splitting is iterative to avoid recursive overflow. The current clock is read again after a
lock wait. These are deliberate corrections to invalid-configuration/prolonged-wait edge cases.

## Limits and circuit breakers

RateLimits preserves capacity, fractional per-token refill, path/user overrides, per-key isolation,
and reset when configuration changes. HistoricalLimits consumes a token only when the start instant
is **strictly before** the age threshold. Both retain the one-minute access-expiring bucket cache.
Nonpositive capacity and negative refill duration fail the IO. The source's zero refill interval is
retained: at an unchanged clock instant it does not refill; after time advances it fills to capacity.

Concurrent configuration reads receive increasing tickets immediately before evaluating the supplied
IO. Reads run outside the limiter mutex. After a read succeeds, the latest accepted snapshot is the
one with the greatest read-start ticket; an older completing read uses that accepted full snapshot.
Accepting a snapshot, selecting/replacing its effective bucket and consuming a token share one short
critical section per limiter instance. Equal snapshots also advance the accepted ticket, so a delayed
read cannot reset an exhausted bucket after a same-value read or an A-to-B-to-A reload. Historical
age checks use the same accepted snapshot. Failed/canceled reads and canceled mutex waiters publish
nothing; cancellation after entering the masked transaction may still consume a token.

This corrects the source's invalidate-and-retry race. The existing IO[Config] API carries no external
revision: tickets order read initiation, not actual revisions in a remote configuration service. An
older-started read returning genuinely newer external data can therefore be superseded by a
later-started accepted read. Callers needing external revision ordering must provide an appropriately
consistent configuration source; the limiter cannot infer revisions from configuration values.

CircuitBreakers retains Armeria configuration, path isolation, reload, half-open/open windows,
fallback handlers and custom outcome classification. `guardEither` treats Left as failure by default;
plain `guard` sees a normally returned Left as successful IO completion. `Resource.ExitCase` does not
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
retained source comment mentioning milliseconds. `makeIO` replaces `makeUIO` and raises
RuntimeException lazily for validation failures. Java temporal conversion/overflow errors remain
possible for unsupported extremes; the reduced source's omitted integrations remain omitted.

The duration reader preserves finite durations, including negative values, and maps every nonfinite
Scala duration to `Duration.ofNanos(Long.MaxValue)` as the source did. This sentinel is not mathematical
infinity. Utility-specific validation still applies afterward.

Global ZIO metrics become an explicit `CacheMetrics` adapter, disabled by default. Metric names,
labels, limiter cache names and the 30-second resource lifetime are retained. Adapter instances
sharing a registry share backing for the same labeled gauge. Application code owns registry creation,
exporter selection and shutdown; no exporter or server is automatically started.
