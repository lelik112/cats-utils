// Provenance: pinned TimeRange source reference; see PROVENANCE.md.
package io.github.lelik112.catsutils.domain

import java.time.{Instant, ZoneId, ZoneOffset}

import io.circe.parser.decode
import io.circe.syntax._
import munit.FunSuite

/** Port of the ten characterization tests added by the source reference, separate from its supplied tests. */
final class TimeRangeCharacterizationTest extends FunSuite {
  private val t0 = Instant.parse("2030-01-01T00:00:00Z")
  private val t1 = t0.plusSeconds(1)
  private val t2 = t0.plusSeconds(2)
  private val t3 = t0.plusSeconds(3)

  private def range(from: Instant, until: Instant): TimeRange =
    TimeRange.make(from, until).fold(fail(_), identity)

  test("includes has half-open boundaries") {
    val value = range(t0, t2)
    assert(value.includes(t0))
    assert(value.includes(t1))
    assert(!value.includes(t2))
    assert(!value.includes(t0.minusSeconds(1)))
  }

  test("make requires the same ZoneId for both boundaries") {
    assert(TimeRange.make(t0.atZone(ZoneOffset.UTC), t1.atZone(ZoneId.of("Europe/Paris"))).isLeft)
  }

  test("adjacent ranges are not intersecting") {
    val a = range(t0, t1)
    val b = range(t1, t2)
    assert(a.adjacent(b) && b.adjacent(a))
    assert(!a.intersects(b))
    assert(a.intersection(b).isEmpty)
  }

  test("intersection keeps only shared instants") {
    assert(range(t0, t2).intersection(range(t1, t3)).contains(range(t1, t2)))
    assert(range(t0, t1).intersection(range(t2, t3)).isEmpty)
  }

  test("containment includes equal range boundaries") {
    val value = range(t0, t2)
    assert(value.contains(value))
    assert(value.contains(range(t1, t2)))
    assert(!range(t1, t2).contains(value))
    assert(value.duration.toNanos == 2000000000L)
  }

  test("union is the enclosing interval, including a possible gap") {
    assert(range(t0, t1).union(range(t2, t3)) == range(t0, t3))
  }

  test("equality and hashing use instants rather than presentation zones") {
    val utc = range(t0, t1)
    val paris = TimeRange
      .make(t0.atZone(ZoneId.of("Europe/Paris")), t1.atZone(ZoneId.of("Europe/Paris")))
      .fold(fail(_), identity)
    assert(utc == paris)
    assert(utc.hashCode == paris.hashCode)
  }

  test("smallest keeps the supplied 1000-nanosecond behavior") {
    assert(TimeRange.smallest(t0.atZone(ZoneOffset.UTC)).duration.toNanos == 1000L)
  }

  test("Circe roundtrip uses from/to and rejects malformed or invalid input") {
    val value = range(t0, t2)
    val json = value.asJson
    assert(json.hcursor.get[String]("to").isRight)
    assert(decode[TimeRange](json.noSpaces).contains(value))
    assert(decode[TimeRange]("""{"from":"2030-01-01T00:00:00Z","to":"2030-01-01T00:00:00Z"}""").isLeft)
    assert(decode[TimeRange]("""{"from":"bad","to":"2030-01-01T00:00:00Z"}""").isLeft)
  }

  test("ordinary callers use make, not generated apply/copy or external subclasses") {
    assertEquals(
      compileErrors(
        """io.github.lelik112.catsutils.domain.TimeRange.make(java.time.Instant.EPOCH, java.time.Instant.EPOCH.plusSeconds(1))"""
      ),
      ""
    )
    assert(
      compileErrors(
        """io.github.lelik112.catsutils.domain.TimeRange(java.time.Instant.EPOCH.atZone(java.time.ZoneOffset.UTC), java.time.Instant.EPOCH.atZone(java.time.ZoneOffset.UTC))"""
      ).nonEmpty
    )
    assert(compileErrors("""io.github.lelik112.catsutils.domain.TimeRange.Largest.copy()""").nonEmpty)
    assert(
      compileErrors(
        """new io.github.lelik112.catsutils.domain.TimeRange(java.time.Instant.EPOCH.atZone(java.time.ZoneOffset.UTC), java.time.Instant.EPOCH.atZone(java.time.ZoneOffset.UTC)) {}"""
      ).nonEmpty
    )
  }
}
