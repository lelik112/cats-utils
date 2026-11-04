package io.github.lelik112.catsutils.domain

import java.time.{DateTimeException, Instant, ZoneId, ZoneOffset}

import munit.CatsEffectSuite

/** Cats Effect adaptation checks, separate from the supplied and imported characterization suites. */
final class TimeRangeIOTest extends CatsEffectSuite {
  private val start = Instant.parse("2030-01-01T00:00:00Z")
  private val end = start.plusSeconds(1)

  test("makeIO returns the same range for instant and zoned boundaries") {
    for {
      instant <- TimeRange.makeIO(start, end)
      zoned <- TimeRange.makeIO(start.atZone(ZoneOffset.UTC), end.atZone(ZoneOffset.UTC))
    } yield {
      assertEquals(TimeRange.make(start, end), Right(instant))
      assertEquals(zoned, instant)
    }
  }

  test("makeIO reports invalid instant bounds as a RuntimeException in the effect") {
    val effect = TimeRange.makeIO(end, start)
    effect.attempt.map {
      case Left(error) =>
        assert(error.getClass == classOf[RuntimeException])
        assertEquals(Some(error.getMessage), TimeRange.make(end, start).left.toOption)
      case Right(value) => fail(s"Expected invalid bounds to fail, obtained $value")
    }
  }

  test("makeIO reports mismatched zones through the effect") {
    val from = start.atZone(ZoneOffset.UTC)
    val until = end.atZone(ZoneId.of("Europe/Paris"))
    val effect = TimeRange.makeIO(from, until)
    effect.attempt.map {
      case Left(error) =>
        assert(error.getClass == classOf[RuntimeException])
        assertEquals(Some(error.getMessage), TimeRange.make(from, until).left.toOption)
      case Right(value) => fail(s"Expected mismatched zones to fail, obtained $value")
    }
  }

  test("instant conversion failures are suspended until the IO runs") {
    // Instant.MIN is outside ZonedDateTime's representable range, so conversion itself throws.
    val effect = TimeRange.makeIO(Instant.MIN, Instant.MIN.plusNanos(1))
    effect.attempt.map {
      case Left(_: DateTimeException) => ()
      case other                      => fail(s"Expected a suspended date-time conversion failure, obtained $other")
    }
  }
}
