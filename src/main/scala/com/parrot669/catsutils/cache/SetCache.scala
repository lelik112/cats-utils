// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils.cache

import java.time.Duration

import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.effect.{Async, Resource}
import cats.syntax.all._

abstract class SetCache[F[_]: cats.Functor, K1, K2, V] {
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
  private final case class Entry[F[_], K, V](state: Map[K, Option[V]], lock: Semaphore[F], weight: Int = 0)

  private object Entry {
    def empty[F[_]: Async, K, V]: F[Entry[F, K, V]] = Semaphore[F](1).map(Entry(Map.empty[K, Option[V]], _))
  }

  private def make[F[_]: Async, K1, K2, V](
    cache:          Cache[F, K1, Nothing, Entry[F, K2, V]],
    invalidateLock: Semaphore[F],
    cacheEmptyKeys: Boolean,
    metrics:        SpecificCacheMetrics[F],
    weigh:          Map[K2, Option[V]] => Option[Int]
  ): SetCache[F, K1, K2, V] = new SetCache[F, K1, K2, V] {
    override def getOrUpdateOptionEither[E](key: K1, keys: Set[K2])(
      fill: Set[K2] => F[Either[E, Option[Map[K2, V]]]]
    ): F[Either[E, Map[K2, V]]] =
      if (keys.isEmpty) Async[F].pure(Right(Map.empty))
      else
        cache
          .get(key)
          .flatMap {
            case Some(entry) if keys.forall(entry.state.contains) =>
              metrics.hit(locked = false).as(Right(entry.state): Either[E, Map[K2, Option[V]]])
            case _                                                =>
              update(key) { state =>
                val missing = keys -- state.keySet
                if (missing.isEmpty)
                  metrics.hit(locked = true).as(Right(state): Either[E, Map[K2, Option[V]]])
                else
                  metrics.load *> Async[F]
                    .defer(fill(missing))
                    .map(_.map {
                      case None           => state
                      case Some(received) =>
                        val values = received.map { case (k, v) => k -> (Some(v): Option[V]) }
                        val absent =
                          if (cacheEmptyKeys) (missing -- received.keySet).map(_ -> Option.empty[V]).toMap
                          else Map.empty[K2, Option[V]]
                        state ++ values ++ absent
                    })
              }
          }
          .map(_.map(_.collect { case (k, Some(v)) if keys(k) => k -> v }))

    override def put(key: K1, values: Map[K2, V]): F[Unit] =
      if (values.isEmpty) Async[F].unit
      else update[Nothing](key)(state => Async[F].pure(Right(state ++ values.map { case (k, v) => k -> Some(v) }))).void

    override def invalidate(key: K1): F[Unit] =
      invalidateLock.permit.use(_ => cache.invalidate(key))

    private def update[E](key: K1)(
      f: Map[K2, Option[V]] => F[Either[E, Map[K2, Option[V]]]]
    ): F[Either[E, Map[K2, Option[V]]]] =
      // Creating a replacement generation shares the commit gate. In particular, expiry must
      // not allow a new entry to appear between an old owner's identity check and weighted commit.
      invalidateLock.permit.use(_ => cache.getOrUpdateValue(key)(Entry.empty[F, K2, V])).flatMap { original =>
        original.lock.permit
          .use { _ =>
            // Immediate expiry or failed admission must not cause an endless lock-identity retry.
            // A detached empty entry serves this request; the identity check below prevents caching it.
            cache.get(key).map(_.getOrElse(original.copy(state = Map.empty[K2, Option[V]]))).flatMap {
              case entry if entry.lock eq original.lock =>
                Async[F]
                  .defer(f(entry.state))
                  .flatTap {
                    case Right(result) =>
                      invalidateLock.permit.use { _ =>
                        cache.get(key).flatMap {
                          // An invalidation or eviction must not let an old loader resurrect the entry.
                          case Some(current) if current eq entry =>
                            Async[F].delay(weigh(result)).flatMap {
                              case Some(weight) => cache.put(key, entry.copy(state = result, weight = weight))
                              // Caffeine accepts only Int entry weights. Return an unrepresentable
                              // result to this caller without retaining any part of the new entry.
                              case None         => cache.invalidate(key)
                            }
                          case _                                 => Async[F].unit
                        }
                      }
                    case Left(_)       => Async[F].unit
                  }
                  .map[Option[Either[E, Map[K2, Option[V]]]]](value => Some(value))
              case _                                    => Async[F].pure(Option.empty[Either[E, Map[K2, Option[V]]]])
            }
          }
          .flatMap {
            case Some(result) => Async[F].pure(result)
            // Release the obsolete lock before retrying against the replacement entry.
            case None         => Async[F].defer(update(key)(f))
          }
      }
  }

  private def weigher[K, V](size: V => Int): Map[K, Option[V]] => Option[Int] = state => {
    val total = state.valuesIterator.foldLeft(0L) { (sum, value) =>
      val weight = value.fold(0)(size)
      require(weight >= 0, "set cache value weight must be nonnegative")
      Math.addExact(sum, weight.toLong)
    }
    Option.when(total <= Int.MaxValue)(total.toInt)
  }

  private def build[F[_]: Async, K1, K2, V](
    cache:          Resource[F, Cache[F, K1, Nothing, Entry[F, K2, V]]],
    name:           String,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F],
    size:           Option[V => Int] = None
  ): Resource[F, SetCache[F, K1, K2, V]] =
    for {
      underlying <- cache
      gate       <- Resource.eval(Semaphore[F](1))
    } yield make(
      underlying,
      gate,
      cacheEmptyKeys,
      SpecificCacheMetrics[F]("set_cache", name, metrics),
      size.fold[Map[K2, Option[V]] => Option[Int]](_ => Some(0))(weigher[K2, V])
    )

  def expireAfterWrite[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterWrite(name, expiration, cacheEmptyKeys, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, None, cacheEmptyKeys, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, None, cacheEmptyKeys, metrics)

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, cacheEmptyKeys, CacheMetrics.noop[F])

  def expireAfterWrite[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(
      Cache.expireAfterWrite[F, K1, Nothing, Entry[F, K2, V]](name, expiration, metrics),
      name,
      cacheEmptyKeys,
      metrics
    )

  def expireAfterWrite[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterWrite(name, expiration, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F])

  def expireAfterWrite[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(
      Cache.expireAfterWrite[F, K1, Nothing, Entry[F, K2, V]](
        name,
        expiration,
        maxWeight,
        (entry: Entry[F, K2, V]) => entry.weight,
        metrics
      ),
      name,
      cacheEmptyKeys,
      metrics,
      Some(size)
    )

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(
      Cache.expireAfterAccess[F, K1, Nothing, Entry[F, K2, V]](name, expiration, refresh, metrics),
      name,
      cacheEmptyKeys,
      metrics
    )

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean
  ): Resource[F, SetCache[F, K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K1, K2, V](
    name:           String,
    expiration:     Duration,
    refresh:        Option[Duration],
    size:           V => Int,
    maxWeight:      Long,
    cacheEmptyKeys: Boolean,
    metrics:        CacheMetrics[F]
  ): Resource[F, SetCache[F, K1, K2, V]] =
    build(
      Cache.expireAfterAccess[F, K1, Nothing, Entry[F, K2, V]](
        name,
        expiration,
        refresh,
        maxWeight,
        (entry: Entry[F, K2, V]) => entry.weight,
        metrics
      ),
      name,
      cacheEmptyKeys,
      metrics,
      Some(size)
    )
}
