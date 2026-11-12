// Adapted from owner-supplied utilities attributed to Bobylev (Бобылев); see PROVENANCE.md.
package com.parrot669.catsutils.cache

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import cats.effect.syntax.all._
import cats.effect.{Async, Deferred, Resource, Sync}
import cats.syntax.all._

import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache, Caffeine}

/** Business errors are explicit Left values and are cached. Raised exceptions are never cached. */
abstract class Cache[F[_]: Sync, K, E, V] {
  def get(key: K): F[Option[V]]
  def getOrUpdate(key: K)(load: => F[Either[E, V]]): F[Either[E, V]]
  def getOrUpdateOption(key: K)(load: => F[Either[E, Option[V]]]): F[Either[E, Option[V]]]
  def put(key: K, value: V): F[Unit]
  def invalidate(key: K): F[Unit]
  def keys: F[Set[K]]
  def size: F[Long]
  def weight: F[Long]

  final def getOrUpdateValue(key: K)(load: => F[V])(implicit infallible: E =:= Nothing): F[V] =
    getOrUpdate(key)(Sync[F].defer(load).map(Right(_))).map(_.fold(infallible, identity))

  final def getOrUpdateOptionValue(key: K)(load: => F[Option[V]])(implicit infallible: E =:= Nothing): F[Option[V]] =
    getOrUpdateOption(key)(Sync[F].defer(load).map(Right(_))).map(_.fold(infallible, identity))
}

object Cache {
  type UCache[F[_], K, V] = Cache[F, K, Nothing, V]
  private val MetricsInterval = 30.seconds

  private sealed trait Signal[+E, +V]
  private final case class Completed[E, V](result: Either[E, Option[V]]) extends Signal[E, V]
  private final case class Failed(error: Throwable)                      extends Signal[Nothing, Nothing]
  private case object Retry                                              extends Signal[Nothing, Nothing]

  private sealed trait Entry[F[_], E, V] {
    def weight: Int
  }
  private final case class Loading[F[_], E, V](done: Deferred[F, Signal[E, V]]) extends Entry[F, E, V] {
    val weight: Int = 1
  }

  private final case class Loaded[F[_], E, V](result: Either[E, V], weight: Int) extends Entry[F, E, V]

  def expireAfterWrite[F[_]: Async, K, E, V](name: String, expiration: Duration): Resource[F, Cache[F, K, E, V]] =
    expireAfterWrite(name, expiration, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K, E, V](name: String, expiration: Duration): Resource[F, Cache[F, K, E, V]] =
    expireAfterAccess(name, expiration, None, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    refresh:    Option[Duration]
  ): Resource[F, Cache[F, K, E, V]] =
    expireAfterAccess(name, expiration, refresh, CacheMetrics.noop[F])

  def make[F[_]: Async, K, E, V](
    name:              String,
    expireAfterWrite:  Option[Duration],
    expireAfterAccess: Option[Duration],
    maxWeight:         Option[Long],
    weigher:           Option[V => Int]
  ): Resource[F, Cache[F, K, E, V]] =
    make(name, expireAfterWrite, expireAfterAccess, maxWeight, weigher, CacheMetrics.noop[F])

  def expireAfterWrite[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    metrics:    CacheMetrics[F]
  ): Resource[F, Cache[F, K, E, V]] = make(name, Some(expiration), None, None, None, metrics)

  def expireAfterWrite[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    maxWeight:  Long,
    weigher:    V => Int
  ): Resource[F, Cache[F, K, E, V]] = expireAfterWrite(name, expiration, maxWeight, weigher, CacheMetrics.noop[F])

  def expireAfterWrite[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    maxWeight:  Long,
    weigher:    V => Int,
    metrics:    CacheMetrics[F]
  ): Resource[F, Cache[F, K, E, V]] = make(name, Some(expiration), None, Some(maxWeight), Some(weigher), metrics)

  def expireAfterAccess[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    refresh:    Option[Duration] = None,
    metrics:    CacheMetrics[F]
  ): Resource[F, Cache[F, K, E, V]] = make(name, refresh, Some(expiration), None, None, metrics)

  def expireAfterAccess[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    refresh:    Option[Duration],
    maxWeight:  Long,
    weigher:    V => Int
  ): Resource[F, Cache[F, K, E, V]] =
    expireAfterAccess(name, expiration, refresh, maxWeight, weigher, CacheMetrics.noop[F])

  def expireAfterAccess[F[_]: Async, K, E, V](
    name:       String,
    expiration: Duration,
    refresh:    Option[Duration],
    maxWeight:  Long,
    weigher:    V => Int,
    metrics:    CacheMetrics[F]
  ): Resource[F, Cache[F, K, E, V]] = make(name, refresh, Some(expiration), Some(maxWeight), Some(weigher), metrics)

  def make[F[_]: Async, K, E, V](
    name:              String,
    expireAfterWrite:  Option[Duration],
    expireAfterAccess: Option[Duration],
    maxWeight:         Option[Long],
    weigher:           Option[V => Int],
    metrics:           CacheMetrics[F]
  ): Resource[F, Cache[F, K, E, V]] = for {
    start  <- Resource.eval(Async[F].monotonic)
    ticker <- Resource.eval(Async[F].delay(new AtomicLong(start.toNanos)))
    tick    = Async[F].monotonic.flatMap(now =>
                Async[F].delay(ticker.accumulateAndGet(now.toNanos, (a: Long, b: Long) => Math.max(a, b))).void
              )
    cache  <- Resource.eval(Async[F].delay {
                require(maxWeight.isDefined == weigher.isDefined, "maxWeight and weigher must be supplied together")
                val builder = Caffeine
                  .newBuilder()
                  .recordStats()
                  .executor((runnable: Runnable) => runnable.run())
                  .ticker(() => ticker.get())
                expireAfterWrite.foreach(builder.expireAfterWrite)
                expireAfterAccess.foreach(builder.expireAfterAccess)
                maxWeight.foreach(builder.maximumWeight)
                weigher.foreach(_ => builder.weigher((_: K, entry: Entry[F, E, V]) => entry.weight))
                builder.build[K, Entry[F, E, V]]()
              })
    result  = new LiveCache(cache, tick, weigher)
    _      <- meter(name, cache, tick, maxWeight, metrics).background
  } yield result

  private def meter[F[_]: Async, K, E, V](
    name:      String,
    cache:     CaffeineCache[K, Entry[F, E, V]],
    tick:      F[Unit],
    maxWeight: Option[Long],
    metrics:   CacheMetrics[F]
  ): F[Unit] = {
    val tags   = Map("name" -> name)
    val record = tick *> Async[F].delay { cache.cleanUp(); cache.stats() }.flatMap { stats =>
      metrics.gauge("cache_hits_total", tags, stats.hitCount().toDouble) *>
        metrics.gauge("cache_misses_total", tags, stats.missCount().toDouble) *>
        Async[F].delay(cache.estimatedSize()).flatMap(n => metrics.gauge("cache_size", tags, n.toDouble)) *>
        maxWeight.traverse_ { maximum =>
          Async[F].delay(weightOf(cache)).flatMap(n => metrics.gauge("cache_weight", tags, n.toDouble)) *>
            metrics.gauge("cache_max_weight", tags, maximum.toDouble)
        }
    }
    (record *> Async[F].sleep(MetricsInterval)).foreverM
  }

  private def weightOf(cache: CaffeineCache[_, _]): Long =
    cache.policy().eviction().toScala.flatMap(_.weightedSize().toScala).getOrElse(0L)

  private final class LiveCache[F[_]: Async, K, E, V](
    cache:   CaffeineCache[K, Entry[F, E, V]],
    tick:    F[Unit],
    weigher: Option[V => Int]
  ) extends Cache[F, K, E, V] {
    // Expiration may remove a Loading placeholder before its loader finishes. Keep its publication
    // generation until completion or explicit mutation; coordinate through Caffeine's per-key compute.
    private val owners = new ConcurrentHashMap[K, Loading[F, E, V]]()
    private def access[A](action: => A): F[A] = tick *> Async[F].delay(action)

    // User code runs before a cache mutation. Caffeine's own weigher is a total field read,
    // so a thrown/negative user weight cannot strand a Loading entry or retire its owner.
    private def loaded(result: Either[E, V]): F[Loaded[F, E, V]] = Async[F].delay {
      val weight = result.fold(_ => 1, value => weigher.fold(1)(_(value)))
      require(weight >= 0, "cache entry weight must be nonnegative")
      Loaded[F, E, V](result, weight)
    }

    private def retire(key: K, loading: Loading[F, E, V]): F[Unit] = access {
      cache
        .asMap()
        .compute(
          key,
          (_, entry) => {
            owners.remove(key, loading)
            if (entry eq loading) null else entry
          }
        )
    }.void

    private def publish(key: K, loading: Loading[F, E, V], result: Either[E, V]): F[Unit] =
      loaded(result).flatMap { completed =>
        access {
          cache.asMap().compute(key, (_, entry) => if (owners.remove(key, loading)) completed else entry)
        }.void
      }

    def get(key: K): F[Option[V]] = access(Option(cache.getIfPresent(key))).flatMap {
      case None                            => Async[F].pure(None)
      case Some(Loaded(result, _))         => Async[F].pure(result.toOption)
      case Some(loading: Loading[F, E, V]) =>
        loading.done.get.map {
          case Completed(Right(value)) => value
          case _                       => None
        }
    }

    def getOrUpdate(key: K)(load: => F[Either[E, V]]): F[Either[E, V]] =
      getOrUpdateOption(key)(Async[F].defer(load).map(_.map(Some(_)))).flatMap {
        case Right(None)        => getOrUpdate(key)(load)
        case Right(Some(value)) => Async[F].pure(Right(value))
        case Left(error)        => Async[F].pure(Left(error))
      }

    def getOrUpdateOption(key: K)(load: => F[Either[E, Option[V]]]): F[Either[E, Option[V]]] =
      Deferred[F, Signal[E, V]].map(Loading(_)).flatMap { loading =>
        Async[F].uncancelable { poll =>
          access(cache.get(key, _ => { owners.put(key, loading); loading })).flatMap {
            case entry if entry eq loading =>
              val abandon = retire(key, loading).guarantee(loading.done.complete(Retry).void)
              poll(Async[F].defer(load)).onCancel(abandon).attempt.flatMap { attempted =>
                val commit = attempted match {
                  case Right(Left(error))        => publish(key, loading, Left(error))
                  case Right(Right(Some(value))) => publish(key, loading, Right(value))
                  case _                         => retire(key, loading)
                }
                commit.attempt.flatMap {
                  case Left(error) =>
                    retire(key, loading).attempt *>
                      loading.done.complete(Failed(error)) *> Async[F].raiseError(error)
                  case Right(_)    =>
                    attempted match {
                      case Right(result) => loading.done.complete(Completed(result)).as(result)
                      case Left(error)   => loading.done.complete(Failed(error)) *> Async[F].raiseError(error)
                    }
                }
              }
            case Loaded(result, _)         => Async[F].pure(result.map(Some(_)))
            case current: Loading[F, E, V] =>
              poll(current.done.get).flatMap {
                case Completed(result) => Async[F].pure(result)
                case Failed(error)     => Async[F].raiseError(error)
                case Retry             => poll(getOrUpdateOption(key)(load))
              }
          }
        }
      }

    def put(key: K, value: V): F[Unit] = loaded(Right(value)).flatMap { completed =>
      access {
        cache.asMap().compute(key, (_, _) => { owners.remove(key); completed })
      }.void
    }
    def invalidate(key: K): F[Unit]    = access {
      cache.asMap().compute(key, (_, _) => { owners.remove(key); null })
    }.void
    def keys: F[Set[K]]                = access { cache.cleanUp(); cache.asMap().keySet().asScala.toSet }
    def size: F[Long]                  = access { cache.cleanUp(); cache.estimatedSize() }
    def weight: F[Long]                = access { cache.cleanUp(); weightOf(cache) }
  }

}
