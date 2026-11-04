// Provenance: owner-supplied utility attributed to Bobylev (Бобылев); see PROVENANCE.md.
// Source attribution and adaptation notes: see PROVENANCE.md.
package com.parrot669.catsutils.cache.ranged

import java.time.Instant
import scala.collection.SortedMap
import scala.collection.immutable.SortedSet

import cats.syntax.functor._

import com.parrot669.catsutils.domain.TimeRange

trait RangeMap[+A] {
  def add[B >: A](range: TimeRange, values: Iterable[(Instant, B)]): RangeMap[B]
  def remove(from: Instant): RangeMap[A]
  def contains(range: TimeRange): Boolean
  def values(range: TimeRange): SortedMap[TimeRange, Iterable[A]]
  def gaps(range: TimeRange): SortedSet[TimeRange]
  def isEmpty: Boolean
  def size: Int
}

// scalafix:off
object RangeMap {

  private type Values[+A] = SortedMap[Instant, Iterable[A]]

  // Based on Discrete Interval Encoding Tree: https://typelevel.org/cats-collections/diet.html
  private[ranged] sealed abstract class AbstractRangeMap[+A](implicit order: Ordering[Instant]) extends RangeMap[A] {

    override final def add[B >: A](range: TimeRange, values: Iterable[(Instant, B)]): AbstractRangeMap[B] = this match {
      case Node(entry, left, right) =>
        val other = Entry(range, values)
        if (entry.range.intersects(other.range) || entry.range.adjacent(other.range)) {
          val merged              = entry.range.union(other.range)
          val (l, start, prepend) = left.until(merged.start)
          val (r, end, append)    = right.from(merged.end)
          val values              = prepend ++ entry.diff(other.range) ++ other.values ++ append
          Node(Entry(mkRange(start, end), values), l, r)
        } else if (other.range.start.isBefore(entry.start)) {
          Node(entry, left.add(range, values), right)
        } else {
          Node(entry, left, right.add(range, values))
        }

      case Empty() => Node(Entry(range, values), Empty(), Empty())
    }

    override final def remove(from: Instant): AbstractRangeMap[A] = this match {
      case Node(entry, left, right) =>
        if (entry.range.includes(from)) {
          if (entry.range.start == from) {
            left
          } else {
            Node(Entry(mkRange(entry.range.start, from), entry.until(from)), left, Empty())
          }
        } else if (from.isBefore(entry.range.start)) {
          left.remove(from)
        } else {
          Node(entry, left, right.remove(from))
        }

      case empty => empty
    }

    override final def contains(range: TimeRange): Boolean = this match {
      case Node(entry, left, right) =>
        if (entry.range.includes(range.start)) entry.range.contains(range)
        else if (range.start.isBefore(entry.range.start)) left.contains(range)
        else right.contains(range)

      case _ => false
    }

    override final def values(range: TimeRange): SortedMap[TimeRange, Iterable[A]] = this match {
      case Node(entry, left, right) =>
        val builder = SortedMap.newBuilder[TimeRange, Iterable[A]](ordering)
        if (range.start.isBefore(entry.range.start)) builder.addAll(left.values(range))
        if (entry.range.intersects(range)) builder.addAll(entry.intersection(range))
        if (range.end.isAfter(entry.range.end)) builder.addAll(right.values(range))
        builder.result()

      case _ => SortedMap.empty(ordering)
    }

    override def size: Int = this match {
      case Node(entry, left, right) => left.size + entry.size + right.size
      case Empty()                  => 0
    }

    override final def gaps(range: TimeRange): SortedSet[TimeRange] = this match {
      case Node(entry, left, right) =>
        if (contains(range)) {
          SortedSet.empty(ordering)
        } else {
          val builder = SortedSet.newBuilder[TimeRange](ordering)
          if (range.start.isBefore(entry.range.start)) {
            val until = if (range.end.isBefore(entry.range.start)) range.end else entry.range.start
            builder.addAll(left.gaps(mkRange(range.start, until)))
          }
          if (range.end.isAfter(entry.range.end)) {
            val from = if (range.start.isAfter(entry.range.end)) range.start else entry.range.end
            builder.addAll(right.gaps(mkRange(from, range.end)))
          }
          builder.result()
        }

      case _ => SortedSet(range)(ordering)
    }

    override final val isEmpty: Boolean = this match {
      case _: Node[_] => false
      case _          => true
    }

    private def from(instant: Instant): (AbstractRangeMap[A], Instant, Values[A]) = this match {
      case Node(entry, left, right) =>
        if (entry.includes(instant)) {
          (right, entry.end, entry.from(instant))
        } else if (instant.isBefore(entry.range.start)) {
          val (remaining, end, data) = left.from(instant)
          (Node(entry, remaining, right), end, data)
        } else {
          right.from(instant)
        }

      case empty => (empty, instant, SortedMap.empty)
    }

    private def until(instant: Instant): (AbstractRangeMap[A], Instant, Values[A]) = this match {
      case Node(entry, left, right) =>
        if (entry.includes(instant) || entry.end == instant) {
          (left, entry.start, entry.until(instant))
        } else if (instant.isBefore(entry.range.start)) {
          left.until(instant)
        } else {
          val (remaining, start, data) = right.until(instant)
          (Node(entry, left, remaining), start, data)
        }

      case empty => (empty, instant, SortedMap.empty)
    }

    private def mkRange(start: Instant, end: Instant): TimeRange =
      TimeRange
        .make(start, end)
        .getOrElse(sys.error("Shouldn't ever happen"))
  }

  private[ranged] final case class Empty()(implicit order: Ordering[Instant]) extends AbstractRangeMap[Nothing]

  private[ranged] final case class Node[+A](
    entry: Entry[A],
    left:  AbstractRangeMap[A],
    right: AbstractRangeMap[A]
  )(implicit order: Ordering[Instant])
      extends AbstractRangeMap[A] {
    override def toString: String = s"Node(${entry.range}, $left, $right)"
  }

  private[ranged] final case class Entry[+A](range: TimeRange, values: Values[A])(implicit order: Ordering[Instant]) {
    private val asc = order.lt(Instant.MIN, Instant.MAX)

    val start: Instant = range.start
    val end: Instant   = range.end

    def includes(instant: Instant): Boolean = range.includes(instant)

    def from(instant: Instant): Values[A] =
      if (asc) values.rangeFrom(instant)
      else values.rangeTo(instant)

    def until(instant: Instant): Values[A] =
      if (asc) values.rangeUntil(instant)
      else values.rangeFrom(instant.minusNanos(1))

    def diff(other: TimeRange): Values[A] =
      until(other.start) ++ from(other.end)

    def intersection(other: TimeRange): Option[(TimeRange, Iterable[A])] =
      range.intersection(other).map {
        case other if range == other => range -> values.values.flatten
        case other if asc            => other -> values.rangeFrom(other.start).rangeUntil(other.end).values.flatten
        case other                   => other -> values.rangeFrom(other.end.minusNanos(1)).rangeTo(other.start).values.flatten
      }

    val size: Int = values.map(_._2.size).sum
  }

  private def ordering(implicit order: Ordering[Instant]): Ordering[TimeRange] =
    if (order.lt(Instant.MIN, Instant.MAX)) TimeRange.ordering
    else TimeRange.ordering.reverse

  private object Entry {
    def apply[A](range: TimeRange, values: Iterable[(Instant, A)])(implicit order: Ordering[Instant]): Entry[A] = {
      val builder = SortedMap.newBuilder[Instant, Iterable[A]]
      builder.addAll(values.filter(x => range.includes(x._1)).groupBy(_._1).fmap(_.map(_._2)))
      Entry(range, builder.result())
    }
  }

  def empty[A](implicit order: Ordering[Instant]): RangeMap[A] = Empty()
}
