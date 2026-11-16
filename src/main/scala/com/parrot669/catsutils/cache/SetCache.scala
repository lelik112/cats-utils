// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils.cache

import java.time.Duration

import cats.effect.std.Semaphore
import cats.effect.{Concurrent, Resource, Sync, Temporal}
import cats.syntax.all._
import cats.{Defer, Functor}

abstract class SetCache[F[_]: Functor, K1, K2, V] {
  final def getOrUpdate(key: K1, keys: Set[K2])(fill: Set[K2] => F[Map[K2, V]]): F[Map[K2, V]] =
    getOrUpdateOption(key, keys)(missing => fill(missing).map(value => Some(value)))

  final def getOrUpdateOption(key: K1, keys: Set[K2])(
    fill: Set[K2] => F[Option[Map[K2, V]]]
  ): F[Map[K2, V]] =
    getOrUpdateOptionEither[Nothing](key, keys)(missing => fill(missing).map(Right(_))).map {
      case Right(values) => values
      case Left(error)   => error
    }

  final def getOrUpdateEither[E](key: K1, keys: Set[K2])(
    fill: Set[K2] => F[Either[E, Map[K2, V]]]
  ): F[Either[E, Map[K2, V]]] =
    getOrUpdateOptionEither(key, keys)(missing => fill(missing).map(_.map(value => Some(value))))

  def getOrUpdateOptionEither[E](key: K1, keys: Set[K2])(
    fill: Set[K2] => F[Either[E, Option[Map[K2, V]]]]
  ): F[Either[E, Map[K2, V]]]

  def put(key: K1, values: Map[K2, V]): F[Unit]
  def invalidate(key: K1): F[Unit]
}

object SetCache {
  private final case class Cell[V](value: Option[V], writtenAt: Long, accessedAt: Long, charge: Int)
  private final case class Entry[F[_], K, V](state: Map[K, Cell[V]], lock: Semaphore[F], weight: Int = 1)

  private object Entry {
    def empty[F[_]: Concurrent, K, V]: F[Entry[F, K, V]] = Semaphore[F](1).map(Entry(Map.empty[K, Cell[V]], _))
  }

  private final case class Policy(write: Option[Long], access: Option[Long]) {
    // Compare elapsed time, never an overflowing absolute deadline. Durations fit signed nanoseconds.
    private def within(now: Long, since: Long, ttl: Long): Boolean =
      java.lang.Long.compareUnsigned(now - since, ttl) < 0

    def fresh[V](cell: Cell[V], now: Long): Boolean =
      write.forall(within(now, cell.writtenAt, _)) && access.forall(within(now, cell.accessedAt, _))
  }

  // Selected hits belong to this call even if they expire while a missing member is loading.
  // Retention is decided separately at publication; new values acquire their age only then.
  private final case class Change[K, V](selected: Map[K, Cell[V]], added: Map[K, Option[V]]) {
    def values: Map[K, Option[V]] = selected.map { case (key, cell) => key -> cell.value } ++ added
  }

  private def make[F[_], K1, K2, V](
    cache:          Cache[F, K1, Nothing, Entry[F, K2, V]],
    invalidateLock: Semaphore[F],
    cacheEmptyKeys: Boolean,
    metrics:        SpecificCacheMetrics[F],
    policy:         Policy,
    size:           V => Int,
    S:              Sync[F]
  )(implicit F: Temporal[F]): SetCache[F, K1, K2, V] = new SetCache[F, K1, K2, V] {
    private implicit val deferred: Defer[F] = S

    private def live(state: Map[K2, Cell[V]], now: Long): Map[K2, Cell[V]] =
      state.filter { case (_, cell) => policy.fresh(cell, now) }

    private def read[E](key: K1, keys: Set[K2])(
      fill: Set[K2] => F[Either[E, Option[Map[K2, V]]]]
    ): F[Either[E, Map[K2, Option[V]]]] =
      update(key) { (state, now) =>
        val missing = keys -- state.keySet
        val touched =
          if (policy.access.isEmpty) state
          else state.map { case (k, cell) => k -> (if (keys(k)) cell.copy(accessedAt = now) else cell) }
        if (missing.isEmpty)
          metrics.hit(locked = true).as(Right(Change(touched, Map.empty[K2, Option[V]])))
        else
          metrics.load *> S
            .defer(fill(missing))
            .map(_.map {
              case None           => Change(touched, Map.empty[K2, Option[V]])
              case Some(received) =>
                val values = received.map { case (k, v) => k -> (Some(v): Option[V]) }
                val absent =
                  if (cacheEmptyKeys) (missing -- received.keySet).map(_ -> Option.empty[V]).toMap
                  else Map.empty[K2, Option[V]]
                Change(touched, values ++ absent)
            })
      }

    override def getOrUpdateOptionEither[E](key: K1, keys: Set[K2])(
      fill: Set[K2] => F[Either[E, Option[Map[K2, V]]]]
    ): F[Either[E, Map[K2, V]]] =
      if (keys.isEmpty) F.pure(Right(Map.empty))
      else {
        val result =
          if (policy.access.nonEmpty) read(key, keys)(fill)
          else
            cache.get(key).flatMap {
              case Some(entry) =>
                F.monotonic.flatMap { now =>
                  val state = live(entry.state, now.toNanos)
                  if (keys.forall(state.contains))
                    metrics
                      .hit(locked = false)
                      .as(Right(state.map { case (k, cell) => k -> cell.value }): Either[E, Map[K2, Option[V]]])
                  else read(key, keys)(fill)
                }
              case None        => read(key, keys)(fill)
            }
        result.map(_.map(_.collect { case (k, Some(v)) if keys(k) => k -> v }))
      }

    override def put(key: K1, values: Map[K2, V]): F[Unit] =
      if (values.isEmpty) F.unit
      else
        update[Nothing](key) { (state, _) =>
          F.pure(Right(Change(state, values.map { case (k, v) => k -> (Some(v): Option[V]) })))
        }.void

    override def invalidate(key: K1): F[Unit] =
      invalidateLock.permit.use(_ => cache.invalidate(key))

    private def publish(key: K1, entry: Entry[F, K2, V], change: Change[K2, V]): F[Unit] =
      invalidateLock.permit.use { _ =>
        cache.get(key).flatMap {
          case Some(current) if current eq entry =>
            // Validate every new member before publishing anything, including after aggregate overflow.
            // Existing cells keep their charge: access hits do not re-run the user's weigher.
            S.delay(change.added.map { case (k, value) =>
              val weight = value.fold(1)(size)
              require(weight >= 0, "set cache value weight must be nonnegative")
              k -> (value, math.max(1, weight))
            }).flatMap { charged =>
              F.monotonic.flatMap { now =>
                val written  = charged.map { case (k, (value, charge)) =>
                  k -> Cell(value, now.toNanos, now.toNanos, charge)
                }
                val retained = live(change.selected ++ written, now.toNanos)
                val total    = math
                  .max(1L, retained.valuesIterator.foldLeft(0L)((sum, cell) => Math.addExact(sum, cell.charge.toLong)))
                if (total <= Int.MaxValue) cache.put(key, entry.copy(state = retained, weight = total.toInt))
                // Whole-group eviction, under the same identity gate as ordinary publication.
                else cache.invalidate(key)
              }
            }
          case _                                 => F.unit
        }
      }

    private def update[E](key: K1)(
      f: (Map[K2, Cell[V]], Long) => F[Either[E, Change[K2, V]]]
    ): F[Either[E, Map[K2, Option[V]]]] =
      // Never hold this gate while awaiting a group permit or executing a user loader.
      invalidateLock.permit.use(_ => cache.getOrUpdateValue(key)(Entry.empty[F, K2, V])).flatMap { original =>
        original.lock.permit
          .use { _ =>
            // A detached entry serves one request without retention; zero TTL/budget cannot loop forever.
            cache.get(key).map(_.getOrElse(original.copy(state = Map.empty[K2, Cell[V]]))).flatMap {
              case entry if entry.lock eq original.lock =>
                F.monotonic
                  .flatMap(now => S.defer(f(live(entry.state, now.toNanos), now.toNanos)))
                  .flatTap {
                    case Right(change) => publish(key, entry, change)
                    case Left(_)       => F.unit
                  }
                  .map(value => Some(value.map(_.values)): Option[Either[E, Map[K2, Option[V]]]])
              case _                                    => F.pure(Option.empty[Either[E, Map[K2, Option[V]]]])
            }
          }
          .flatMap {
            case Some(result) => F.pure(result)
            // Release the obsolete permit before retrying against a replacement generation.
            case None         => S.defer(update(key)(f))
          }
      }
  }

  private def build[F[_], K1, K2, V](
    name:           String,
    write:          Option[Duration],
    access:         Option[Duration],
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F],
    S:              Sync[F]
  )(implicit F: Temporal[F]): Resource[F, SetCache[F, K1, K2, V]] = {
    def nanos(duration: Duration): Long = {
      require(!duration.isNegative, "set cache duration must be nonnegative")
      duration.toNanos
    }
    for {
      policy     <- Resource.eval(S.delay {
                      require(maxWeight >= 0, "set cache maximum weight must be nonnegative")
                      Policy(write.map(nanos), access.map(nanos))
                    })
      underlying <- Cache.make[F, K1, Nothing, Entry[F, K2, V]](
                      name,
                      write,
                      access,
                      Some(maxWeight),
                      Some((entry: Entry[F, K2, V]) => entry.weight),
                      metrics
                    )(S, F)
      gate       <- Resource.eval(Semaphore[F](1))
    } yield make(underlying, gate, cacheEmptyKeys, SpecificCacheMetrics[F]("set_cache", name, metrics), policy, size, S)
  }

  def expireAfterWrite[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterWrite(name, expiration, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F](Temporal[F]))

  def expireAfterWrite[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterWrite(name, expiration, (_: V) => 1, maxWeight, cacheEmptyKeys, metrics)

  def expireAfterWrite[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterWrite(name, expiration, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F](Temporal[F]))

  def expireAfterWrite[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(name, Some(expiration), None, size, maxWeight, cacheEmptyKeys, metrics, Sync[F])(Temporal[F])

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, None, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F](Temporal[F]))

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, None, maxWeight, cacheEmptyKeys, metrics)

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F](Temporal[F]))

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, (_: V) => 1, maxWeight, cacheEmptyKeys, metrics)

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F](Temporal[F]))

  def expireAfterAccess[F[_]: Sync: Temporal, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(name, refresh, Some(expiration), size, maxWeight, cacheEmptyKeys, metrics, Sync[F])(Temporal[F])
}
