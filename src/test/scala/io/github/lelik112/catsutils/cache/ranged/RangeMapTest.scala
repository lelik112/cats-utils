// Port of all 31 supplied RangeMap tests; fixtures and expected behaviors retained. See PROVENANCE.md.
package io.github.lelik112.catsutils.cache.ranged

import java.time.Instant
import scala.collection.immutable.SortedSet

import io.github.lelik112.catsutils.cache.ranged.RangeMap.{AbstractRangeMap, Node}
import io.github.lelik112.catsutils.domain.TimeRange

// scalafix:off
final class RangeMapTest extends munit.FunSuite {

  test("add and return values") {
    var map = RangeMap.empty[Int]

    map = map.add(range("06:00", "10:00"), values("06:30" -> 1, "07:00" -> 2, "09:59" -> 3))
    map = map.add(range("12:00", "14:00"), values("13:30" -> 4, "13:59" -> 5))

    assert(map.values(range("03:00", "06:00")).isEmpty)
    assertEquals(map.values(range("06:15", "09:00")).values.flatten.toList, List(1, 2))
    assertEquals(map.values(range("07:00", "13:45")).values.flatten.toList, List(2, 3, 4))
  }

  test("add and return values (reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("06:00", "10:00"), values("06:30" -> 1, "07:00" -> 2, "09:59" -> 3))
    map = map.add(range("12:00", "14:00"), values("13:30" -> 4, "13:59" -> 5))

    assert(map.values(range("03:00", "06:00")).isEmpty)
    assertEquals(map.values(range("06:15", "09:00")).values.flatten.toList, List(2, 1))
    assertEquals(map.values(range("07:00", "13:45")).values.flatten.toList, List(4, 3, 2))
  }

  test("combine adjacent entries (left)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))
    map = map.add(range("03:00", "06:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(2, 3))
  }

  test("combine adjacent entries (left, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))
    map = map.add(range("03:00", "06:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(3, 2))
  }

  test("combine adjacent entries (right)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("03:00", "06:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(2, 3))
  }

  test("combine adjacent entries (right, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("03:00", "06:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(3, 2))
  }

  test("combine intersecting entries (left)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))
    map = map.add(range("03:00", "07:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(1, 2, 4))
  }

  test("combine intersecting entries (left, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))
    map = map.add(range("03:00", "07:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(4, 2, 1))
  }

  test("combine intersecting entries (right)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("03:00", "07:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(1, 2, 3, 4))
  }

  test("combine intersecting entries (right, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("03:00", "07:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 3, "07:00" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(4, 3, 2, 1))
  }

  test("combine entry with subrange") {
    var map = RangeMap.empty[Int]

    map = map.add(range("05:00", "10:00"), values("05:15" -> 1, "07:30" -> 2, "09:59" -> 5))
    map = map.add(range("07:00", "08:00"), values("07:30" -> 3, "07:45" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(1, 3, 4, 5))
  }

  test("combine entry with subrange (reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("05:00", "10:00"), values("05:15" -> 1, "07:30" -> 2, "09:59" -> 5))
    map = map.add(range("07:00", "08:00"), values("07:30" -> 3, "07:45" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(5, 4, 3, 1))
  }

  test("combine entry with superrange") {
    var map = RangeMap.empty[Int]

    map = map.add(range("05:00", "10:00"), values("05:15" -> 1, "09:59" -> 4))
    map = map.add(range("00:00", "02:00"), values("00:15" -> 0))
    map = map.add(range("13:00", "15:00"), values("13:15" -> 5))
    map = map.add(range("03:00", "12:00"), values("07:30" -> 2, "07:45" -> 3))

    assertEquals(height(map), 2)
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(2, 3))
  }

  test("combine entry with superrange (reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("05:00", "10:00"), values("05:15" -> 1, "09:59" -> 4))
    map = map.add(range("00:00", "02:00"), values("00:15" -> 0))
    map = map.add(range("13:00", "15:00"), values("13:15" -> 5))
    map = map.add(range("03:00", "12:00"), values("07:30" -> 2, "07:45" -> 3))

    assertEquals(height(map), 2)
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(3, 2))
  }

  test("combine entries when gap is filled (left)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("06:00", "10:00"), values("06:30" -> 4, "07:00" -> 5))
    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 2)

    map = map.add(range("04:00", "06:00"), values("05:30" -> 3))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(2, 3, 4))
  }

  test("combine entries when gap is filled (left, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("06:00", "10:00"), values("06:30" -> 4, "07:00" -> 5))
    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 2)

    map = map.add(range("04:00", "06:00"), values("05:30" -> 3))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(4, 3, 2))
  }

  test("combine entries when gap is filled (right)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 4, "07:00" -> 5))

    assertEquals(height(map), 2)

    map = map.add(range("04:00", "06:00"), values("05:30" -> 3))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(2, 3, 4))
  }

  test("combine entries when gap is filled (right, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 4, "07:00" -> 5))

    assertEquals(height(map), 2)

    map = map.add(range("04:00", "06:00"), values("05:30" -> 3))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:30", "06:45")).values.flatten.toList, List(4, 3, 2))
  }

  test("combine entries when gap is filled with overlap (left)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("06:00", "10:00"), values("06:30" -> 5, "07:30" -> 6))
    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 2)

    map = map.add(range("03:30", "07:30"), values("03:59" -> 3, "05:30" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(1, 3, 4, 6))
  }

  test("combine entries when gap is filled with overlap (left, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("06:00", "10:00"), values("06:30" -> 5, "07:30" -> 6))
    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))

    assertEquals(height(map), 2)

    map = map.add(range("03:30", "07:30"), values("03:59" -> 3, "05:30" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(6, 4, 3, 1))
  }

  test("combine entries when gap is filled with overlap (right)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 5, "07:30" -> 6))

    assertEquals(height(map), 2)

    map = map.add(range("03:30", "07:30"), values("03:59" -> 3, "05:30" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(1, 3, 4, 6))
  }

  test("combine entries when gap is filled with overlap (right, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("03:00", "04:00"), values("03:15" -> 1, "03:59" -> 2))
    map = map.add(range("06:00", "10:00"), values("06:30" -> 5, "07:30" -> 6))

    assertEquals(height(map), 2)

    map = map.add(range("03:30", "07:30"), values("03:59" -> 3, "05:30" -> 4))

    assertEquals(height(map), 1)
    assertEquals(map.values(range("03:00", "10:00")).values.flatten.toList, List(6, 4, 3, 1))
  }

  test("remove values from the root") {
    var map = RangeMap.empty[Int]
    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.remove(instant("08:00"))
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(1, 2, 3))
    assertEquals(map.remove(instant("05:00")).isEmpty, true)
  }

  test("remove values from the root (reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)
    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.remove(instant("08:00"))
    assertEquals(map.values(range("03:00", "12:00")).values.flatten.toList, List(3, 2, 1))
    assertEquals(map.remove(instant("05:00")).isEmpty, true)
  }

  test("remove values (left)") {
    var map = RangeMap.empty[Int]

    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.add(range("12:00", "15:00"), values("12:00" -> 6, "13:00" -> 7, "14:00" -> 8))
    assertEquals(height(map), 2)

    map = map.remove(instant("08:00"))
    assertEquals(height(map), 1)
    assertEquals(map.values(range("05:00", "15:00")).values.flatten.toList, List(1, 2, 3))

    assertEquals(map.remove(instant("05:00")).isEmpty, true)
  }

  test("remove values (left, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)

    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.add(range("12:00", "15:00"), values("12:00" -> 6, "13:00" -> 7, "14:00" -> 8))
    assertEquals(height(map), 2)

    map = map.remove(instant("08:00"))
    assertEquals(height(map), 1)
    assertEquals(map.values(range("05:00", "15:00")).values.flatten.toList, List(3, 2, 1))

    assertEquals(map.remove(instant("05:00")).isEmpty, true)
  }

  test("remove values (right)") {
    var map = RangeMap.empty[Int]
    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.add(range("12:00", "15:00"), values("12:00" -> 6, "13:00" -> 7, "14:00" -> 8))
    map = map.remove(instant("13:00"))
    assertEquals(map.values(range("05:00", "15:00")).values.flatten.toList, List(1, 2, 3, 4, 5, 6))
  }

  test("remove values (right, reversed)") {
    var map = RangeMap.empty[Int](Ordering[Instant].reverse)
    map = map.add(range("05:00", "10:00"), values("05:00" -> 1, "06:00" -> 2, "07:00" -> 3, "08:00" -> 4, "09:00" -> 5))
    map = map.add(range("12:00", "15:00"), values("12:00" -> 6, "13:00" -> 7, "14:00" -> 8))
    map = map.remove(instant("13:00"))
    assertEquals(map.values(range("05:00", "15:00")).values.flatten.toList, List(6, 5, 4, 3, 2, 1))
  }

  test("tell if range is contained") {
    var map = RangeMap.empty[Int]

    map = map.add(range("10:00", "15:00"), Nil)
    map = map.add(range("03:00", "08:00"), Nil)
    map = map.add(range("17:00", "22:00"), Nil)

    assertEquals(map.contains(range("02:00", "03:00")), false)
    assertEquals(map.contains(range("02:00", "04:00")), false)
    assertEquals(map.contains(range("02:00", "08:00")), false)
    assertEquals(map.contains(range("02:00", "10:00")), false)

    assertEquals(map.contains(range("03:00", "04:00")), true)
    assertEquals(map.contains(range("03:00", "08:00")), true)
    assertEquals(map.contains(range("03:00", "10:00")), false)

    assertEquals(map.contains(range("04:00", "05:00")), true)
    assertEquals(map.contains(range("04:00", "08:00")), true)
    assertEquals(map.contains(range("04:00", "10:00")), false)

    assertEquals(map.contains(range("08:00", "09:00")), false)
    assertEquals(map.contains(range("08:00", "10:00")), false)

    assertEquals(map.contains(range("10:00", "12:00")), true)
    assertEquals(map.contains(range("10:00", "15:00")), true)
    assertEquals(map.contains(range("10:00", "17:00")), false)

    assertEquals(map.contains(range("12:00", "13:00")), true)
    assertEquals(map.contains(range("12:00", "15:00")), true)
    assertEquals(map.contains(range("12:00", "17:00")), false)

    assertEquals(map.contains(range("15:00", "16:00")), false)
    assertEquals(map.contains(range("15:00", "18:00")), false)

    assertEquals(map.contains(range("17:00", "18:00")), true)
    assertEquals(map.contains(range("17:00", "22:00")), true)
    assertEquals(map.contains(range("17:00", "23:00")), false)

    assertEquals(map.contains(range("18:00", "19:00")), true)
    assertEquals(map.contains(range("18:00", "22:00")), true)
    assertEquals(map.contains(range("18:00", "23:00")), false)
    assertEquals(map.contains(range("22:00", "23:00")), false)
  }

  test("return gaps") {
    var map = RangeMap.empty[Int]

    map = map.add(range("10:00", "15:00"), Nil)
    map = map.add(range("03:00", "08:00"), Nil)
    map = map.add(range("17:00", "22:00"), Nil)

    assert(map.gaps(range("04:00", "05:00")).isEmpty)
    assert(map.gaps(range("10:00", "15:00")).isEmpty)
    assert(map.gaps(range("17:00", "20:00")).isEmpty)

    assertEquals(map.gaps(range("02:00", "05:00")), SortedSet(range("02:00", "03:00")))
    assertEquals(map.gaps(range("02:00", "09:00")), SortedSet(range("02:00", "03:00"), range("08:00", "09:00")))
    assertEquals(map.gaps(range("03:00", "10:00")), SortedSet(range("08:00", "10:00")))
    assertEquals(map.gaps(range("05:00", "12:00")), SortedSet(range("08:00", "10:00")))
    assertEquals(map.gaps(range("08:00", "16:00")), SortedSet(range("08:00", "10:00"), range("15:00", "16:00")))
    assertEquals(map.gaps(range("12:00", "23:00")), SortedSet(range("15:00", "17:00"), range("22:00", "23:00")))

    assertEquals(
      map.gaps(range("00:00", "23:59")),
      SortedSet(
        range("00:00", "03:00"),
        range("08:00", "10:00"),
        range("15:00", "17:00"),
        range("22:00", "23:59")
      )
    )
  }

  test("return gaps (reversed)") {
    var map = RangeMap.empty[Int]
    val order = TimeRange.ordering.reverse

    map = map.add(range("10:00", "15:00"), Nil)
    map = map.add(range("03:00", "08:00"), Nil)
    map = map.add(range("17:00", "22:00"), Nil)

    assert(map.gaps(range("04:00", "05:00")).isEmpty)
    assert(map.gaps(range("10:00", "15:00")).isEmpty)
    assert(map.gaps(range("17:00", "20:00")).isEmpty)

    assertEquals(map.gaps(range("02:00", "05:00")), SortedSet(range("02:00", "03:00"))(order))
    assertEquals(map.gaps(range("02:00", "09:00")), SortedSet(range("02:00", "03:00"), range("08:00", "09:00"))(order))
    assertEquals(map.gaps(range("03:00", "10:00")), SortedSet(range("08:00", "10:00"))(order))
    assertEquals(map.gaps(range("05:00", "12:00")), SortedSet(range("08:00", "10:00"))(order))
    assertEquals(map.gaps(range("08:00", "16:00")), SortedSet(range("08:00", "10:00"), range("15:00", "16:00"))(order))
    assertEquals(map.gaps(range("12:00", "23:00")), SortedSet(range("15:00", "17:00"), range("22:00", "23:00"))(order))

    assertEquals(
      map.gaps(range("00:00", "23:59")),
      SortedSet(
        range("00:00", "03:00"),
        range("08:00", "10:00"),
        range("15:00", "17:00"),
        range("22:00", "23:59")
      )(order)
    )
  }

  private def height(map: RangeMap[_]): Int =
    map.asInstanceOf[AbstractRangeMap[_]] match {
      case Node(_, left, right) => 1 + Math.max(height(left), height(right))
      case _                    => 0
    }

  private def instant(time: String): Instant =
    Instant.parse(s"2025-02-17T$time:00Z")

  private def range(from: String, until: String): TimeRange =
    TimeRange
      .make(instant(from), instant(until))
      .getOrElse(sys.error("boom"))

  private def values(values: (String, Int)*): Iterable[(Instant, Int)] =
    values.map { case (string, value) =>
      instant(string) -> value
    }
}
