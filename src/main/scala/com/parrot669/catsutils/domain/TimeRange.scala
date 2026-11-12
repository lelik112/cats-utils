// Provenance: reduced TimeRange reference accompanying Bobylev (Бобылев) utilities; see PROVENANCE.md.
// Source attribution and adaptation notes: see PROVENANCE.md.
package com.parrot669.catsutils.domain

import java.time.{Duration, Instant, ZoneOffset, ZonedDateTime}
import scala.reflect.ClassTag

import cats.effect.Sync

import io.circe.{Decoder, DecodingFailure, Encoder}

sealed abstract case class TimeRange(from: ZonedDateTime, until: ZonedDateTime) {
  val start: Instant = from.toInstant
  val end: Instant   = until.toInstant

  def duration: Duration = Duration.between(start, end)

  def includes(time: ZonedDateTime): Boolean =
    includes(time.toInstant)

  def includes(instant: Instant): Boolean =
    start.compareTo(instant) <= 0 && end.isAfter(instant)

  def contains(other: TimeRange): Boolean =
    includes(other.start) && end.compareTo(other.end) >= 0

  def adjacent(other: TimeRange): Boolean = {
    val (left, right) = if (start.isBefore(other.start)) (this, other) else (other, this)
    left.end == right.start
  }

  def union(other: TimeRange): TimeRange = {
    val start = if (this.start.isBefore(other.start)) this.start else other.start
    val end   = if (this.end.isAfter(other.end)) this.end else other.end
    new TimeRange(start.atZone(from.getZone), end.atZone(from.getZone)) {}
  }

  def intersects(other: TimeRange): Boolean =
    includes(other.start) || other.includes(start)

  def intersection(from: Instant, until: Option[Instant]): Option[TimeRange] =
    TimeRange.makeOption(
      from = if (from.isAfter(start)) from.atZone(this.from.getZone) else this.from,
      until = until.filter(_.isBefore(end)).map(_.atZone(this.from.getZone)).getOrElse(this.until)
    )

  def intersection(from: Instant): Option[TimeRange] =
    intersection(from, None)

  def intersection(from: Instant, to: Instant): Option[TimeRange] =
    intersection(from, Some(to))

  def intersection(other: TimeRange): Option[TimeRange] =
    intersection(other.start, Some(other.end))

  def offset: ZoneOffset = from.getOffset

  override def hashCode: Int =
    (start, end).hashCode

  override def equals(other: Any): Boolean =
    eq(other.asInstanceOf[AnyRef]) || // scalafix:ok
      TimeRange.Tag.unapply(other).fold(false) { other =>
        start == other.start && end == other.end
      }

  override def toString: String =
    s"$from–$until"
}

object TimeRange {

  private val Tag = implicitly[ClassTag[TimeRange]]

  val JsonFrom: String = "from"
  val JsonTo: String   = "to"

  val Largest: TimeRange = new TimeRange(
    Instant.ofEpochMilli(Long.MinValue).atZone(ZoneOffset.UTC),
    Instant.ofEpochMilli(Long.MaxValue).atZone(ZoneOffset.UTC)
  ) {}

  // Добавил на всякий случай миллисекунду вместо нано, что бы не упасть там, где равенство для from и until (в строковом представлении) не ожидается
  // Retained source behavior: 1000 nanoseconds is one microsecond, despite the millisecond comment above.
  def smallest(from: ZonedDateTime): TimeRange =
    new TimeRange(from, from.plusNanos(1000L)) {}

  def make(from: ZonedDateTime, until: ZonedDateTime): Either[String, TimeRange] = for {
    _ <- Either.cond(until.isAfter(from), (), s"from must be before until: from $from - until $until")
    _ <- Either.cond(
           from.getZone == until.getZone,
           (),
           s"from and until must be in the same time zone: from $from - until $until"
         )
  } yield new TimeRange(from, until) {}

  def make(from: Instant, until: Instant): Either[String, TimeRange] =
    make(from.atZone(ZoneOffset.UTC), until.atZone(ZoneOffset.UTC))

  def makeOption(from: ZonedDateTime, until: ZonedDateTime): Option[TimeRange] =
    make(from, until).toOption

  def makeOption(from: Instant, until: Instant): Option[TimeRange] =
    make(from, until).toOption

  /** Invalid ranges fail the effect with RuntimeException; validation remains suspended until execution. */
  def makeF[F[_]: Sync](from: ZonedDateTime, until: ZonedDateTime): F[TimeRange] =
    Sync[F].defer(Sync[F].fromEither(make(from, until).left.map(new RuntimeException(_))))

  def makeF[F[_]: Sync](from: Instant, until: Instant): F[TimeRange] =
    Sync[F].defer(Sync[F].fromEither(make(from, until).left.map(new RuntimeException(_))))

  implicit val ordering: Ordering[TimeRange] = Ordering.by(range => (range.start, range.end))

  implicit val encoder: Encoder[TimeRange] =
    Encoder.forProduct2[TimeRange, ZonedDateTime, ZonedDateTime](JsonFrom, JsonTo) { range =>
      (range.from, range.until)
    }

  implicit val decoder: Decoder[TimeRange] = cursor =>
    for {
      from  <- cursor.get[ZonedDateTime](JsonFrom)
      to    <- cursor.get[ZonedDateTime](JsonTo)
      range <- TimeRange.make(from, to).left.map(DecodingFailure(_, Nil))
    } yield range
}
