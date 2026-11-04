// Port of the three supplied TimeRange tests; original fixtures, assertions and titles retained.
// The first supplied title says < although its inputs exercise reversed bounds. See PROVENANCE.md.
package io.github.lelik112.catsutils.domain

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS

final class TimeRangeTest extends munit.FunSuite {

  test("forbid creating values when from < until") {
    assert(TimeRange.make(Instant.now(), Instant.now().minus(1, SECONDS)).isLeft)
  }

  test("forbid creating values when from = until") {
    val now = Instant.now()
    assert(TimeRange.make(now, now).isLeft)
  }

  test("allow creating values when from < until") {
    assert(TimeRange.make(Instant.now().minus(1, SECONDS), Instant.now()).isRight)
  }
}
