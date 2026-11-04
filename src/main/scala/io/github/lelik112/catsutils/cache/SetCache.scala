// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package io.github.lelik112.catsutils.cache

import java.time.Duration

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all._

trait SetCache[K1, K2, V] {
  final def getOrUpdate(key: K1, keys: Set[K2])(fill: Set[K2] => IO[Map[K2, V]]): IO[Map[K2, V]] =
    getOrUpdateOption(key, keys)(missing => fill(missing).map(Some(_)))

  final def getOrUpdateOption(key: K1, keys: Set[K2])(
      fill: Set[K2] => IO[Option[Map[K2, V]]]
  ): IO[Map[K2, V]] =
    getOrUpdateOptionEither[Nothing](key, keys)(missing => fill(missing).map(Right(_))).map {
      case Right(values) => values
      case Left(error)   => error
    }

  final def getOrUpdateEither[E](key: K1, keys: Set[K2])(
      fill: Set[K2] => IO[Either[E, Map[K2, V]]]
  ): IO[Either[E, Map[K2, V]]] =
    getOrUpdateOptionEither(key, keys)(missing => fill(missing).map(_.map(Some(_))))

  def getOrUpdateOptionEither[E](key: K1, keys: Set[K2])(
      fill: Set[K2] => IO[Either[E, Option[Map[K2, V]]]]
  ): IO[Either[E, Map[K2, V]]]

  def put(key: K1, values: Map[K2, V]): IO[Unit]
  def invalidate(key: K1): IO[Unit]
}

object SetCache {
  final private case class Entry[K, V](state: Map[K, Option[V]], lock: Semaphore[IO], weight: Int = 0)

  private object Entry {
    def empty[K, V]: IO[Entry[K, V]] = Semaphore[IO](1).map(Entry(Map.empty[K, Option[V]], _))
  }

  private def make[K1, K2, V](
      cache: Cache[K1, Nothing, Entry[K2, V]],
      invalidateLock: Semaphore[IO],
      cacheEmptyKeys: Boolean,
      metrics: SpecificCacheMetrics,
      weigh: Map[K2, Option[V]] => Option[Int]
  ): SetCache[K1, K2, V] = new SetCache[K1, K2, V] {
    override def getOrUpdateOptionEither[E](key: K1, keys: Set[K2])(
        fill: Set[K2] => IO[Either[E, Option[Map[K2, V]]]]
    ): IO[Either[E, Map[K2, V]]] =
      if (keys.isEmpty) IO.pure(Right(Map.empty))
      else
        cache
          .get(key)
          .flatMap {
            case Some(entry) if keys.forall(entry.state.contains) =>
              metrics.hit(locked = false).as(Right(entry.state): Either[E, Map[K2, Option[V]]])
            case _ =>
              update(key) { state =>
                val missing = keys -- state.keySet
                if (missing.isEmpty)
                  metrics.hit(locked = true).as(Right(state): Either[E, Map[K2, Option[V]]])
                else
                  metrics.load *> IO
                    .defer(fill(missing))
                    .map(_.map {
                      case None => state
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

    override def put(key: K1, values: Map[K2, V]): IO[Unit] =
      if (values.isEmpty) IO.unit
      else update[Nothing](key)(state => IO.pure(Right(state ++ values.map { case (k, v) => k -> Some(v) }))).void

    override def invalidate(key: K1): IO[Unit] =
      invalidateLock.permit.use(_ => cache.invalidate(key))

    private def update[E](key: K1)(
        f: Map[K2, Option[V]] => IO[Either[E, Map[K2, Option[V]]]]
    ): IO[Either[E, Map[K2, Option[V]]]] =
      // Creating a replacement generation shares the commit gate. In particular, expiry must
      // not allow a new entry to appear between an old owner's identity check and weighted commit.
      invalidateLock.permit.use(_ => cache.getOrUpdateValue(key)(Entry.empty[K2, V])).flatMap { original =>
        original.lock.permit
          .use { _ =>
            // Immediate expiry or failed admission must not cause an endless lock-identity retry.
            // A detached empty entry serves this request; the identity check below prevents caching it.
            cache.get(key).map(_.getOrElse(original.copy(state = Map.empty[K2, Option[V]]))).flatMap {
              case entry if entry.lock eq original.lock =>
                IO.defer(f(entry.state))
                  .flatTap {
                    case Right(result) =>
                      invalidateLock.permit.use { _ =>
                        cache.get(key).flatMap {
                          // An invalidation or eviction must not let an old loader resurrect the entry.
                          case Some(current) if current eq entry =>
                            IO.delay(weigh(result)).flatMap {
                              case Some(weight) => cache.put(key, entry.copy(state = result, weight = weight))
                              // Caffeine accepts only Int entry weights. Return an unrepresentable
                              // result to this caller without retaining any part of the new entry.
                              case None => cache.invalidate(key)
                            }
                          case _ => IO.unit
                        }
                      }
                    case Left(_) => IO.unit
                  }
                  .map(Some(_))
              case _ => IO.pure(None)
            }
          }
          .flatMap {
            case Some(result) => IO.pure(result)
            // Release the obsolete lock before retrying against the replacement entry.
            case None => IO.defer(update(key)(f))
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

  private def build[K1, K2, V](
      cache: Resource[IO, Cache[K1, Nothing, Entry[K2, V]]],
      name: String,
      cacheEmptyKeys: Boolean,
      metrics: CacheMetrics,
      size: Option[V => Int] = None
  ): Resource[IO, SetCache[K1, K2, V]] =
    for {
      underlying <- cache
      gate <- Resource.eval(Semaphore[IO](1))
    } yield make(
      underlying,
      gate,
      cacheEmptyKeys,
      SpecificCacheMetrics("set_cache", name, metrics),
      size.fold[Map[K2, Option[V]] => Option[Int]](_ => Some(0))(weigher[K2, V])
    )

  def expireAfterWrite[K1, K2, V](
      name: String,
      expiration: Duration,
      cacheEmptyKeys: Boolean,
      metrics: CacheMetrics = CacheMetrics.noop
  ): Resource[IO, SetCache[K1, K2, V]] =
    build(Cache.expireAfterWrite[K1, Nothing, Entry[K2, V]](name, expiration, metrics), name, cacheEmptyKeys, metrics)

  def expireAfterWrite[K1, K2, V](
      name: String,
      expiration: Duration,
      size: V => Int,
      maxWeight: Long,
      cacheEmptyKeys: Boolean
  ): Resource[IO, SetCache[K1, K2, V]] =
    expireAfterWrite(name, expiration, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop)

  def expireAfterWrite[K1, K2, V](
      name: String,
      expiration: Duration,
      size: V => Int,
      maxWeight: Long,
      cacheEmptyKeys: Boolean,
      metrics: CacheMetrics
  ): Resource[IO, SetCache[K1, K2, V]] =
    build(
      Cache.expireAfterWrite[K1, Nothing, Entry[K2, V]](
        name,
        expiration,
        maxWeight,
        (entry: Entry[K2, V]) => entry.weight,
        metrics
      ),
      name,
      cacheEmptyKeys,
      metrics,
      Some(size)
    )

  def expireAfterAccess[K1, K2, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration] = None,
      cacheEmptyKeys: Boolean,
      metrics: CacheMetrics = CacheMetrics.noop
  ): Resource[IO, SetCache[K1, K2, V]] =
    build(
      Cache.expireAfterAccess[K1, Nothing, Entry[K2, V]](name, expiration, refresh, metrics),
      name,
      cacheEmptyKeys,
      metrics
    )

  def expireAfterAccess[K1, K2, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration],
      size: V => Int,
      maxWeight: Long,
      cacheEmptyKeys: Boolean
  ): Resource[IO, SetCache[K1, K2, V]] =
    expireAfterAccess(name, expiration, refresh, size, maxWeight, cacheEmptyKeys, CacheMetrics.noop)

  def expireAfterAccess[K1, K2, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration],
      size: V => Int,
      maxWeight: Long,
      cacheEmptyKeys: Boolean,
      metrics: CacheMetrics
  ): Resource[IO, SetCache[K1, K2, V]] =
    build(
      Cache.expireAfterAccess[K1, Nothing, Entry[K2, V]](
        name,
        expiration,
        refresh,
        maxWeight,
        (entry: Entry[K2, V]) => entry.weight,
        metrics
      ),
      name,
      cacheEmptyKeys,
      metrics,
      Some(size)
    )
}
