// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils.cache.ranged

import java.time.{Duration, Instant}
import scala.collection.SortedMap

import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.effect.{Async, Deferred, Outcome, Ref, Resource}
import cats.syntax.all._

import com.parrot669.catsutils.cache.{Cache, CacheMetrics, SpecificCacheMetrics}
import com.parrot669.catsutils.domain.TimeRange

import fs2.Stream

abstract class RangeCache[F[_]: cats.effect.Concurrent, K, V] {
  final def getOrUpdate(key: K, range: TimeRange)(
    fill: TimeRange => F[Iterable[(Instant, V)]]
  ): F[List[V]] = getOrUpdateOption(key, range)(gap => fill(gap).map(value => Some(value)))

  final def getOrUpdateOption(key: K, range: TimeRange)(
    fill: TimeRange => F[Option[Iterable[(Instant, V)]]]
  ): F[List[V]] = stream(key, range)(fill).compile.toList

  final def getOrUpdateEither[E](key: K, range: TimeRange)(
    fill: TimeRange => F[Either[E, Iterable[(Instant, V)]]]
  ): F[Either[E, List[V]]] = getOrUpdateOptionEither(key, range)(gap => fill(gap).map(_.map(value => Some(value))))

  final def getOrUpdateOptionEither[E](key: K, range: TimeRange)(
    fill: TimeRange => F[Either[E, Option[Iterable[(Instant, V)]]]]
  ): F[Either[E, List[V]]] = streamEither(key, range)(fill).compile.toList.map(_.sequence)

  final def stream(key: K, range: TimeRange)(
    fill: TimeRange => F[Option[Iterable[(Instant, V)]]]
  ): Stream[F, V] = streamEither[Nothing](key, range)(gap => fill(gap).map(Right(_))).map {
    case Right(value) => value
    case Left(error)  => error
  }

  /** Emits at most one business error, then ends and cancels outstanding parallel loads. */
  def streamEither[E](key: K, range: TimeRange)(
    fill: TimeRange => F[Either[E, Option[Iterable[(Instant, V)]]]]
  ): Stream[F, Either[E, V]]

  def put(key: K, range: TimeRange, values: Iterable[(Instant, V)]): F[Unit]
  def invalidate(key: K, from: Instant): F[Unit]
  final def invalidate(key: K): F[Unit] = invalidate(key, Instant.MIN)
  def keys: F[Set[K]]
  def size: F[Long]
  def weight: F[Long]
}

object RangeCache {
  sealed abstract case class Config(
    expireAfterWrite:  Option[Duration],
    expireAfterAccess: Option[Duration],
    parallelism:       Int,
    maxGap:            Option[Duration], // Split big gaps into maxGap pieces.
    ignoreRanges:      Option[Duration], // Combine gaps separated by small data intervals.
    skipLast:          Option[Duration], // Skip gaps that start within this interval before now.
    maxWeight:         Option[Long],
    trimToNow:         Boolean
  ) {
    private def copy(
      parallelism:  Int = parallelism,
      maxGap:       Option[Duration] = maxGap,
      ignoreRanges: Option[Duration] = ignoreRanges,
      skipLast:     Option[Duration] = skipLast,
      maxWeight:    Option[Long] = maxWeight,
      trimToNow:    Boolean = trimToNow
    ): Config = new Config(
      expireAfterWrite,
      expireAfterAccess,
      parallelism,
      maxGap,
      ignoreRanges,
      skipLast,
      maxWeight,
      trimToNow
    ) {}

    def withParallelism(value: Int): Config = {
      require(value > 0, "parallelism must be positive")
      copy(parallelism = value)
    }

    def withMaxGap(duration: Duration): Config = {
      require(!duration.isNegative && !duration.isZero, "maxGap must be positive")
      copy(maxGap = Some(duration))
    }

    def withIgnoreRanges(duration: Duration): Config = {
      require(!duration.isNegative, "ignoreRanges must be nonnegative")
      copy(ignoreRanges = Some(duration))
    }

    def withSkipLast(duration: Duration): Config = {
      require(!duration.isNegative, "skipLast must be nonnegative")
      copy(skipLast = Some(duration))
    }

    def withMaxWeight(value: Long): Config = {
      require(value >= 0, "maxWeight must be nonnegative")
      copy(maxWeight = Some(value))
    }

    def withTrimToNow: Config = copy(trimToNow = true)
  }

  object Config {
    def expireAfterWrite(duration: Duration): Config =
      new Config(Some(duration), None, 1, None, None, None, None, false) {}

    def expireAfterAccess(duration: Duration): Config =
      new Config(None, Some(duration), 1, None, None, None, None, false) {}
  }

  private final case class Entry[F[_], A](state: RangeMap[A], lock: Semaphore[F], staleFrom: Ref[F, Instant]) {
    def add(range:      TimeRange, values: Iterable[(Instant, A)]): Entry[F, A] = copy(state = state.add(range, values))
    def markStale(from: Instant): F[Unit] = staleFrom.update(current => if (from.isBefore(current)) from else current)
  }

  private object Entry {
    def empty[F[_]: Async, A](implicit order: Ordering[Instant]): F[Entry[F, A]] =
      (Semaphore[F](1), Ref.of[F, Instant](Instant.MAX)).mapN(Entry(RangeMap.empty[A], _, _))
  }

  private sealed trait Part[A]
  private final case class Cached[A](values: Iterable[A])                                      extends Part[A]
  private final case class Loaded[A](range: TimeRange, values: Option[Iterable[(Instant, A)]]) extends Part[A]
  private final class BusinessLoadFailure                                                      extends RuntimeException(null, null, false, false)
  private final class CanceledLoad                                                             extends RuntimeException(null, null, false, false)

  def make[F[_]: Async, K, V](name: String, config: Config)(implicit
    order: Ordering[Instant]
  ): Resource[F, RangeCache[F, K, V]] =
    make(name, config, CacheMetrics.noop[F])

  def make[F[_]: Async, K, V](
    name:    String,
    config:  Config,
    metrics: CacheMetrics[F]
  )(implicit order: Ordering[Instant]): Resource[F, RangeCache[F, K, V]] = {
    val cache = (config.expireAfterWrite, config.maxWeight) match {
      case (Some(duration), Some(weight)) =>
        Cache.expireAfterWrite[F, K, Nothing, Entry[F, V]](name, duration, weight, (_: Entry[F, V]).state.size, metrics)
      case (Some(duration), None)         =>
        Cache.expireAfterWrite[F, K, Nothing, Entry[F, V]](name, duration, metrics)
      case (None, Some(weight))           =>
        Cache.expireAfterAccess[F, K, Nothing, Entry[F, V]](
          name,
          config.expireAfterAccess.get,
          None,
          weight,
          (_: Entry[F, V]).state.size,
          metrics
        )
      case (None, None)                   =>
        Cache.expireAfterAccess[F, K, Nothing, Entry[F, V]](name, config.expireAfterAccess.get, None, metrics)
    }
    for {
      underlying <- cache
      gate       <- Resource.eval(Semaphore[F](1))
    } yield implementation(underlying, gate, config, SpecificCacheMetrics[F]("range_cache", name, metrics))
  }

  private def implementation[F[_]: Async, K, V](
    cache:          Cache[F, K, Nothing, Entry[F, V]],
    invalidateLock: Semaphore[F],
    config:         Config,
    metrics:        SpecificCacheMetrics[F]
  )(implicit order: Ordering[Instant]): RangeCache[F, K, V] = new RangeCache[F, K, V] {
    private val asc                           = order.lt(Instant.MIN, Instant.MAX)
    private val ordering: Ordering[TimeRange] = new Ordering[TimeRange] {
      private val delegate                                  = if (asc) TimeRange.ordering else TimeRange.ordering.reverse
      override def compare(x: TimeRange, y: TimeRange): Int =
        if (x.intersects(y)) 0 // Bigger gaps must consume data intervals.
        else delegate.compare(x, y)
    }

    override def streamEither[E](key: K, requested: TimeRange)(
      fill: TimeRange => F[Either[E, Option[Iterable[(Instant, V)]]]]
    ): Stream[F, Either[E, V]] = Stream.eval(Deferred[F, Either[Throwable, E]]).flatMap { loadFailure =>
      val marker   = new BusinessLoadFailure
      val canceled = new CanceledLoad
      trimmed(requested) { range =>
        Stream.eval((cache.get(key).flatMap(_.traverse(clean)), Async[F].realTimeInstant).tupled).flatMap {
          case (Some(entry), now) if gaps(entry.state, range, now).isEmpty =>
            Stream.eval(metrics.hit(locked = false)) >> values[E](entry.state.values(range).values.flatten)
          case _                                                           =>
            Stream.resource(locked(key)).flatMap { entry =>
              // Child finalizers must finish before the key permit is released.
              Stream.resource(Supervisor[F](await = false)).flatMap { supervisor =>
                Stream.eval(Async[F].realTimeInstant).flatMap { now =>
                  gaps(entry.state, range, now) match {
                    case Nil     =>
                      Stream.eval(metrics.hit(locked = true)) >> values[E](entry.state.values(range).values.flatten)
                    case missing =>
                      val initial                                                  = SortedMap.empty[TimeRange, Option[Iterable[V]]](ordering) ++ missing.map(_ -> None)
                      val withData                                                 = initial ++ entry.state.values(range).iterator.map { case (part, data) =>
                        part -> Some(data)
                      }
                      // Reapply gaps after intervals so an interval cannot replace a consuming gap.
                      val parts                                                    = withData ++ missing.map(_ -> None)
                      def load(part: (TimeRange, Option[Iterable[V]])): F[Part[V]] = part match {
                        case (_, Some(data)) => Async[F].pure(Cached(data))
                        case (gap, None)     =>
                          supervisor.supervise(Async[F].defer(fill(gap))).flatMap(_.join).flatMap {
                            case Outcome.Succeeded(result) =>
                              result.flatMap {
                                case Right(data) => Async[F].pure(Loaded(gap, data))
                                case Left(error) => loadFailure.complete(Right(error)) *> Async[F].never[Part[V]]
                              }
                            // An independent interrupt signal bypasses parEvalMap's ordered result queue.
                            // The watcher is the only failure source; it cancels this waiting worker as well.
                            case Outcome.Errored(error)    => loadFailure.complete(Left(error)) *> Async[F].never[Part[V]]
                            // Canceling the worker's join externally does not enter this branch.
                            case Outcome.Canceled()        => loadFailure.complete(Left(canceled)) *> Async[F].never[Part[V]]
                          }
                      }
                      val source                                                   = Stream.emits(parts.toList).covary[F]
                      val loaded                                                   =
                        if (config.parallelism == 1 || parts.size == 1) source.evalMap(load)
                        else source.parEvalMap(config.parallelism)(load)

                      Stream.eval(metrics.load) >> loaded
                        .evalMapAccumulate(entry) { (previous, result) =>
                          result match {
                            case Cached(data)            => Async[F].pure(previous -> data)
                            case Loaded(gap, Some(data)) =>
                              val updated = previous.add(gap, data)
                              commit(key, previous, updated).map { committed =>
                                // The caller that started before invalidation still receives its loaded data.
                                committed -> updated.state.values(gap).values.flatten
                              }
                            case Loaded(_, None)         => Async[F].pure(previous -> Iterable.empty[V])
                          }
                        }
                        .map(_._2)
                        .flatMap(values[E])
                  }
                }
              }
            }
        }
      }.interruptWhen(loadFailure.get.map {
        case Left(error) => Left(error): Either[Throwable, Unit]
        case Right(_)    => Left(marker): Either[Throwable, Unit]
      }).handleErrorWith {
        case error if error eq canceled => Stream.eval(Async[F].canceled *> Async[F].never[Either[E, V]])
        case error if error eq marker   =>
          Stream.eval(loadFailure.get).flatMap {
            case Right(error) => Stream.emit(Left(error): Either[E, V])
            case Left(error)  => Stream.raiseError[F](error)
          }
        case error                      => Stream.raiseError[F](error)
      }
    }

    override def put(key: K, range: TimeRange, values: Iterable[(Instant, V)]): F[Unit] =
      locked(key).use(entry => commit(key, entry, entry.add(range, values)).void)

    override def invalidate(key: K, from: Instant): F[Unit] =
      invalidateLock.permit.use { _ =>
        cache.get(key).flatMap {
          case None        => Async[F].unit
          case Some(entry) =>
            // Never wait for a loader while holding the invalidation gate.
            Resource.make(entry.lock.tryAcquire)(acquired => if (acquired) entry.lock.release else Async[F].unit).use {
              acquired =>
                entry.markStale(from) *> (if (acquired)
                                            clean(entry).flatMap(fresh =>
                                              if (fresh.state.isEmpty) cache.invalidate(key) else cache.put(key, fresh)
                                            )
                                          else Async[F].unit)
            }
        }
      }

    override def keys: F[Set[K]] = cache.keys
    override def size: F[Long]   = cache.size
    override def weight: F[Long] = cache.weight

    private def values[E](data: Iterable[V]): Stream[F, Either[E, V]] =
      Stream.emits(data.toList).map(value => Right(value): Either[E, V])

    private def trimmed[A](range: TimeRange)(f: TimeRange => Stream[F, A]): Stream[F, A] =
      Stream.eval(Async[F].realTimeInstant).flatMap { now =>
        if (config.trimToNow && range.end.isAfter(now))
          TimeRange.makeOption(range.start, now).fold[Stream[F, A]](Stream.empty)(f)
        else f(range)
      }

    private def gaps(state: RangeMap[V], range: TimeRange, now: Instant): List[TimeRange] = {
      val missing = config.skipLast.fold(state.gaps(range)) { skip =>
        val from = now.minus(skip)
        if (asc) state.gaps(range).takeWhile(_.start.isBefore(from))
        else state.gaps(range).dropWhile(!_.start.isBefore(from))
      }
      split(merge(missing.toList))
    }

    private def merge(gaps: List[TimeRange]): List[TimeRange] = config.ignoreRanges.fold(gaps) { duration =>
      gaps match {
        case x :: y :: rest if distance(x, y).compareTo(duration) <= 0 => merge(x.union(y) :: rest)
        case head :: tail                                              => head :: merge(tail)
        case Nil                                                       => Nil
      }
    }

    private def split(gaps: List[TimeRange]): List[TimeRange] = config.maxGap.fold(gaps) { duration =>
      gaps.flatMap { gap =>
        // Iteration avoids a stack overflow for a long requested range with a small maxGap.
        val builder = List.newBuilder[TimeRange]
        var start   = gap.start
        while (Duration.between(start, gap.end).compareTo(duration) > 0) {
          val end = start.plus(duration)
          builder ++= TimeRange.makeOption(start, end)
          start = end
        }
        builder ++= TimeRange.makeOption(start, gap.end)
        builder.result()
      }
    }

    private def distance(x: TimeRange, y: TimeRange): Duration =
      if (x.end.isBefore(y.start)) Duration.between(x.end, y.start) else Duration.between(y.end, x.start)

    private def locked(key: K): Resource[F, Entry[F, V]] =
      Resource
        .makeFull[F, (Entry[F, V], Semaphore[F])] { poll =>
          def acquire: F[(Entry[F, V], Semaphore[F])] =
            cache.getOrUpdateValue(key)(Entry.empty[F, V]).flatMap { original =>
              poll(original.lock.acquire) *> invalidateLock.permit
                .use { _ =>
                  cache.get(key).flatMap {
                    case Some(current) if current.lock eq original.lock =>
                      // A new lock owner starts a new invalidation generation after removing stale data.
                      (clean(current), Ref.of[F, Instant](Instant.MAX))
                        .mapN((fresh, stale) => fresh.copy(staleFrom = stale))
                        .flatTap(cache.put(key, _))
                        .map[Option[Entry[F, V]]](value => Some(value))
                    // Expired or unadmitted entries still let this request progress without caching.
                    case None                                           =>
                      Ref
                        .of[F, Instant](Instant.MAX)
                        .map[Option[Entry[F, V]]](stale => Some(Entry(RangeMap.empty[V], original.lock, stale)))
                    case _                                              => Async[F].pure(Option.empty[Entry[F, V]])
                  }
                }
                .onError { case _ => original.lock.release }
                .flatMap {
                  case Some(fresh) => Async[F].pure(fresh -> original.lock)
                  case None        => original.lock.release *> poll(Async[F].cede) *> Async[F].defer(acquire)
                }
            }
          acquire
        } { case (_, lock) => lock.release }
        .map(_._1)

    private def clean(entry: Entry[F, V]): F[Entry[F, V]] = entry.staleFrom.get.map {
      case from if from == Instant.MAX => entry
      case from                        => entry.copy(state = entry.state.remove(from))
    }

    private def commit(key: K, previous: Entry[F, V], updated: Entry[F, V]): F[Entry[F, V]] =
      invalidateLock.permit.use { _ =>
        clean(updated).flatTap { fresh =>
          cache.get(key).flatMap {
            case Some(current) if current eq previous => cache.put(key, fresh)
            case _                                    => Async[F].unit
          }
        }
      }
  }
}
