package io.github.lelik112.catsutils.cache.ranged

import java.time.Instant

import io.github.lelik112.catsutils.domain.TimeRange

import munit.FunSuite

/** Additional boundary characterization, kept separate from all 31 supplied RangeMap tests. */
final class RangeMapBoundaryTest extends FunSuite {
  private val origin = Instant.parse("2030-01-01T00:00:00Z")

  private def instant(nanos: Long): Instant = origin.plusNanos(nanos)

  private def range(from: Long, until: Long): TimeRange =
    TimeRange.make(instant(from), instant(until)).fold(fail(_), identity)

  for ((name, order) <- List("ascending" -> Ordering[Instant], "descending" -> Ordering[Instant].reverse)) {
    test(s"half-open insertion filters outside timestamps and counts duplicate values ($name)") {
      val map = RangeMap
        .empty[String](order)
        .add(
          range(0, 3),
          List(-1L -> "before", 0L -> "first", 0L -> "second", 2L -> "last", 3L -> "until")
            .map { case (time, value) => instant(time) -> value }
        )
      val expected = if (name == "ascending") List("first", "second", "last") else List("last", "first", "second")
      assertEquals(map.values(range(-1, 4)).values.flatten.toList, expected)
      assertEquals(map.size, 3)
      assertEquals(map.values(range(0, 1)).values.flatten.toList, List("first", "second"))
      assert(map.values(range(3, 4)).isEmpty)
    }

    test(s"a replacement interval preserves values at its excluded upper boundary ($name)") {
      val map = RangeMap
        .empty[String](order)
        .add(
          range(0, 4),
          List(0L -> "before", 1L -> "old", 2L -> "after", 3L -> "last")
            .map { case (time, value) => instant(time) -> value }
        )
        .add(range(1, 2), List(instant(1) -> "new", instant(2) -> "outside"))

      val expected =
        if (name == "ascending") List("before", "new", "after", "last")
        else List("last", "after", "new", "before")
      assertEquals(map.values(range(0, 4)).values.flatten.toList, expected)
      assertEquals(map.size, 4)
      assert(map.contains(range(0, 4)))
      assert(map.gaps(range(0, 4)).isEmpty)
    }

    test(s"empty values still record covered intervals and gap iteration follows the requested order ($name)") {
      val query = range(0, 10)
      val empty = RangeMap.empty[Int](order)
      assert(empty.isEmpty)
      assertEquals(empty.size, 0)
      assertEquals(empty.gaps(query).toList, List(query))

      val map = empty.add(range(2, 4), Nil).add(range(6, 8), Nil)
      assert(!map.isEmpty)
      assertEquals(map.size, 0)
      assert(map.contains(range(2, 4)))
      assert(!map.contains(range(2, 8)))
      val expectedGaps = List(range(0, 2), range(4, 6), range(8, 10))
      val expectedEntries = List(range(2, 4), range(6, 8))
      assertEquals(map.gaps(query).toList, if (name == "ascending") expectedGaps else expectedGaps.reverse)
      assertEquals(map.values(query).keys.toList, if (name == "ascending") expectedEntries else expectedEntries.reverse)
      assert(map.values(query).values.forall(_.isEmpty))
    }

    test(s"removing inside a gap discards every later interval and retains the earlier coverage ($name)") {
      val map = RangeMap
        .empty[Int](order)
        .add(range(0, 2), List(instant(0) -> 1, instant(1) -> 2))
        .add(range(4, 6), List(instant(4) -> 3, instant(5) -> 4))
        .add(range(8, 10), List(instant(8) -> 5))
      val removed = map.remove(instant(3))
      assertEquals(
        removed.values(range(0, 10)).values.flatten.toList,
        if (name == "ascending") List(1, 2) else List(2, 1)
      )
      assertEquals(removed.size, 2)
      assert(removed.contains(range(0, 2)))
      assertEquals(removed.gaps(range(0, 10)).toList, List(range(2, 10)))
      assertEquals(map.remove(instant(2)).values(range(0, 10)), removed.values(range(0, 10)))
      assert(map.remove(instant(0)).isEmpty)
    }

    test(s"removing at a nanosecond boundary excludes exactly that timestamp ($name)") {
      val map = RangeMap.empty[Int](order).add(range(0, 3), List(instant(0) -> 1, instant(1) -> 2, instant(2) -> 3))
      val removed = map.remove(instant(2))
      assertEquals(
        removed.values(range(0, 3)).values.flatten.toList,
        if (name == "ascending") List(1, 2) else List(2, 1)
      )
      assertEquals(removed.values(range(0, 3)).keys.toList, List(range(0, 2)))
      assertEquals(removed.gaps(range(0, 3)).toList, List(range(2, 3)))
    }
  }
}
