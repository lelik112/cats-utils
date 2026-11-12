# Testing

Use **Java 21**. From the repository root, run:

```sh
./run-sbt clean compile Test/compile test scalafmtCheckAll scalafmtSbtCheck
```

The executable runner selects Java 21 on macOS or uses `JAVA_HOME`/`java` elsewhere.
It downloads the pinned public sbt launcher and Maven Central dependencies into ignored
`.runtime` caches. For an existing cache, set `CATSUTILS_RUNTIME_DIR` to its absolute path.
An offline build requires those dependencies to be cached already.

`scalafmtCheckAll` includes both README `scala mdoc` examples through
`Compile / scalafmt / unmanagedSources`; Markdown is not a compiler input. To apply
the same PARROT 669 style to sources and examples, run `./run-sbt scalafmtAll scalafmtSbt`.
`scalafmtSbtCheck` checks build files. No mdoc plugin or runtime is required.

The suite contains **270 tests**: 120 scenarios from the supplied utilities/support reference
and 150 adaptation or regression scenarios. Tests cover all seven utilities plus TimeRange,
configuration parsing and metrics context. Source comments distinguish supplied cases from
added coverage. Nine effect-polymorphism tests instantiate all effectful public utilities with
`Kleisli[IO, String, *]`, including loader cancellation, ordered range streams, context isolation
and background meter lifetime. Four additional capability tests compile generic factory wrappers
without `Async` evidence and exercise the weaker APIs, including `SyncIO` for the token bucket and
a custom RangeCache facade. The underlying IO is owned only by the test runner. Assertions and
test behavior are retained; two test fixture identifiers use neutral example values.

Regression coverage includes loader/waiter cancellation, resource finalizers, typed versus
raised errors, expiration and invalidation generations, ordered parallel range loads, limiter
reload races, and checked aggregate weights. Deterministic effect-clock tests use Cats Effect
TestControl; selected concurrency regressions repeat multiple controlled schedules.

For a focused suite, for example:

```sh
./run-sbt 'testOnly com.parrot669.catsutils.LimiterReloadTest'
./run-sbt 'testOnly com.parrot669.catsutils.cache.ranged.RangeCacheTest'
```

Test XML is generated under ignored `target/test-reports`. Passing these checks establishes
the covered scenarios, not every possible interleaving, input extreme or application integration.
No local logs, reports, caches, generated documentation or compiled artifacts are tracked.
`publish / skip` remains enabled; the build does not configure a Maven publication.
