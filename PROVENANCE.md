# Provenance and third-party attribution

## Owner-supplied reference

This project adapts seven utilities from an owner-supplied ZIO reference: RateLimits,
HistoricalLimits, Cache, SetCache, RangeMap, RangeCache and CircuitBreakers.
**Bobylev (Бобылев)** is the utility author attribution supplied by the repository owner.
The supplied material does not establish per-file authorship history; this project preserves
that attribution without inventing ownership claims.

Exact source lineage and verification evidence are retained in the owner's private ledger.
Private repository identifiers, internal integrations and local review reports are not included
in this public source tree. The supporting TimeRange and duration reader are reduced excerpts
that accompanied the reference.

The adaptation replaces ZIO with Cats Effect IO, Resource and FS2. It changes APIs,
cancellation/resource handling, concurrent cache generations, limiter configuration ordering
and checked weight accounting as described in [MIGRATION.md](MIGRATION.md). Build support,
explicit metrics/context adapters, OrderedConfig and additional regressions are new adaptation
work. Supplied scenarios remain distinguishable from added tests; see [TESTING.md](TESTING.md).

No project-wide license has been selected. The third-party terms below apply to their identified
upstream portions and do not assign a license to the repository as a whole.

## http4s token-bucket code

The retained source acknowledgment identifies http4s Throttle as the basis of the token-bucket
logic in [RateLimits.scala](src/main/scala/io/github/lelik112/catsutils/RateLimits.scala).
The upstream source carries **Copyright 2014 http4s.org** and Apache License 2.0 terms.

The adaptation changes the effect and result APIs, uses atomic Ref state updates, and preserves
the supplied utility's refill/validation behavior. It does not include http4s's HTTP middleware,
responses or other components. The modification notice is also retained next to TokenBucket.

The official upstream files were verified at public http4s commit
`41dae411e8055cf1a41bf9f14f827f6d87e46dc4`:

- [Throttle source](https://github.com/http4s/http4s/blob/41dae411e8055cf1a41bf9f14f827f6d87e46dc4/server/shared/src/main/scala/org/http4s/server/middleware/Throttle.scala)
- [Upstream license](https://github.com/http4s/http4s/blob/41dae411e8055cf1a41bf9f14f827f6d87e46dc4/LICENSE), reproduced in [third-party-licenses/http4s-LICENSE](third-party-licenses/http4s-LICENSE)
- [Upstream notice](https://github.com/http4s/http4s/blob/41dae411e8055cf1a41bf9f14f827f6d87e46dc4/NOTICE), with the relevant http4s attribution retained in [third-party-licenses/http4s-NOTICE](third-party-licenses/http4s-NOTICE)

That commit identifies the license-verification snapshot, not an asserted original copying
revision. The supplied acknowledgment linked the `series/0.23` branch.

## Other retained acknowledgments

RangeMap retains its [cats-collections Discrete Interval Encoding Tree (DIET)](https://typelevel.org/cats-collections/diet.html)
design acknowledgment. This is not a claim that all RangeMap code was copied from that project,
or an assignment of unverified copied-code license terms. cats-collections is not a runtime
dependency of this module.

The supplied ledger also attributed an unrelated string helper to
[Lift StringHelpers](https://github.com/lift/framework/blob/a3075e0676d60861425281427aa5f57c02c3b0bc/core/util/src/main/scala/net/liftweb/util/StringHelpers.scala#L91).
That helper remains excluded. This acknowledgment does not imply that Lift code is compiled
or distributed here.

## Supporting excerpts and exclusions

TimeRange retains the supplied zoned/instant semantics and JSON fields. Regional-date helpers,
day rounding and the positive-count month-start factory were already omitted from the support
excerpt. The duration reader includes only the supplied excerpt.

JWT, HTTP/auth providers, application defaults, business adapters, tracing/exporters, Kafka,
Cassandra, data exports, event-hub invalidation, ConfigMapping/State, generic matcher/JSON/time/
math/collection helpers, unused newtypes and the unrelated string helper are outside this
module. No unavailable private integration has been replaced by a fabricated application type.

The test ticker helper remains in `com.linecorp.armeria.client.circuitbreaker` to access the
public Armeria dependency's package-private ticker hook. That namespace does not establish
its authorship or a separate license grant. The helper is test-only.

Public dependencies retain their own terms. Dependency coordinates in `build.sbt` do not
settle rights in the owner-supplied source or assign those dependencies' licenses to this project.
