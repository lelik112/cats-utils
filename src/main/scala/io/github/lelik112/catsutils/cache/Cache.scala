// Adapted from owner-supplied utilities attributed to Bobylev (Бобылев); see PROVENANCE.md.
package io.github.lelik112.catsutils.cache

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all._
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache, Caffeine}

/** Business errors are explicit Left values and are cached. Raised exceptions are never cached. */
trait Cache[K, E, V] {
  def get(key: K): IO[Option[V]]
  def getOrUpdate(key: K)(load: => IO[Either[E, V]]): IO[Either[E, V]]
  def getOrUpdateOption(key: K)(load: => IO[Either[E, Option[V]]]): IO[Either[E, Option[V]]]
  def put(key: K, value: V): IO[Unit]
  def invalidate(key: K): IO[Unit]
  def keys: IO[Set[K]]
  def size: IO[Long]
  def weight: IO[Long]

  final def getOrUpdateValue(key: K)(load: => IO[V])(implicit infallible: E =:= Nothing): IO[V] =
    getOrUpdate(key)(IO.defer(load).map(Right(_))).map(_.fold(infallible, identity))

  final def getOrUpdateOptionValue(key: K)(load: => IO[Option[V]])(implicit infallible: E =:= Nothing): IO[Option[V]] =
    getOrUpdateOption(key)(IO.defer(load).map(Right(_))).map(_.fold(infallible, identity))
}

object Cache {
  type UCache[K, V] = Cache[K, Nothing, V]
  private val MetricsInterval = 30.seconds

  sealed private trait Signal[+E, +V]
  final private case class Completed[E, V](result: Either[E, Option[V]]) extends Signal[E, V]
  final private case class Failed(error: Throwable) extends Signal[Nothing, Nothing]
  private case object Retry extends Signal[Nothing, Nothing]

  sealed private trait Entry[E, V] {
    def weight: Int
  }
  final private case class Loading[E, V](done: Deferred[IO, Signal[E, V]]) extends Entry[E, V] {
    val weight: Int = 1
  }
  final private case class Loaded[E, V](result: Either[E, V], weight: Int) extends Entry[E, V]

  def expireAfterWrite[K, E, V](
      name: String,
      expiration: Duration,
      metrics: CacheMetrics = CacheMetrics.noop
  ): Resource[IO, Cache[K, E, V]] = make(name, Some(expiration), None, None, None, metrics)

  def expireAfterWrite[K, E, V](
      name: String,
      expiration: Duration,
      maxWeight: Long,
      weigher: V => Int
  ): Resource[IO, Cache[K, E, V]] = expireAfterWrite(name, expiration, maxWeight, weigher, CacheMetrics.noop)

  def expireAfterWrite[K, E, V](
      name: String,
      expiration: Duration,
      maxWeight: Long,
      weigher: V => Int,
      metrics: CacheMetrics
  ): Resource[IO, Cache[K, E, V]] = make(name, Some(expiration), None, Some(maxWeight), Some(weigher), metrics)

  def expireAfterAccess[K, E, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration] = None,
      metrics: CacheMetrics = CacheMetrics.noop
  ): Resource[IO, Cache[K, E, V]] = make(name, refresh, Some(expiration), None, None, metrics)

  def expireAfterAccess[K, E, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration],
      maxWeight: Long,
      weigher: V => Int
  ): Resource[IO, Cache[K, E, V]] = expireAfterAccess(name, expiration, refresh, maxWeight, weigher, CacheMetrics.noop)

  def expireAfterAccess[K, E, V](
      name: String,
      expiration: Duration,
      refresh: Option[Duration],
      maxWeight: Long,
      weigher: V => Int,
      metrics: CacheMetrics
  ): Resource[IO, Cache[K, E, V]] = make(name, refresh, Some(expiration), Some(maxWeight), Some(weigher), metrics)

  def make[K, E, V](
      name: String,
      expireAfterWrite: Option[Duration],
      expireAfterAccess: Option[Duration],
      maxWeight: Option[Long],
      weigher: Option[V => Int],
      metrics: CacheMetrics = CacheMetrics.noop
  ): Resource[IO, Cache[K, E, V]] = for {
    start <- Resource.eval(IO.monotonic)
    ticker <- Resource.eval(IO.delay(new AtomicLong(start.toNanos)))
    tick = IO.monotonic.flatMap(now =>
      IO.delay(ticker.accumulateAndGet(now.toNanos, (a: Long, b: Long) => Math.max(a, b))).void
    )
    cache <- Resource.eval(IO.delay {
      require(maxWeight.isDefined == weigher.isDefined, "maxWeight and weigher must be supplied together")
      val builder = Caffeine
        .newBuilder()
        .recordStats()
        .executor((runnable: Runnable) => runnable.run())
        .ticker(() => ticker.get())
      expireAfterWrite.foreach(builder.expireAfterWrite)
      expireAfterAccess.foreach(builder.expireAfterAccess)
      maxWeight.foreach(builder.maximumWeight)
      weigher.foreach(_ => builder.weigher((_: K, entry: Entry[E, V]) => entry.weight))
      builder.build[K, Entry[E, V]]()
    })
    result = new LiveCache(cache, tick, weigher)
    _ <- meter(name, cache, tick, maxWeight, metrics).background
  } yield result

  private def meter[K, E, V](
      name: String,
      cache: CaffeineCache[K, Entry[E, V]],
      tick: IO[Unit],
      maxWeight: Option[Long],
      metrics: CacheMetrics
  ): IO[Nothing] = {
    val tags = Map("name" -> name)
    val record = tick *> IO.delay { cache.cleanUp(); cache.stats() }.flatMap { stats =>
      metrics.gauge("cache_hits_total", tags, stats.hitCount().toDouble) *>
        metrics.gauge("cache_misses_total", tags, stats.missCount().toDouble) *>
        IO.delay(cache.estimatedSize()).flatMap(n => metrics.gauge("cache_size", tags, n.toDouble)) *>
        maxWeight.traverse_ { maximum =>
          IO.delay(weightOf(cache)).flatMap(n => metrics.gauge("cache_weight", tags, n.toDouble)) *>
            metrics.gauge("cache_max_weight", tags, maximum.toDouble)
        }
    }
    (record *> IO.sleep(MetricsInterval)).foreverM
  }

  private def weightOf(cache: CaffeineCache[_, _]): Long =
    cache.policy().eviction().toScala.flatMap(_.weightedSize().toScala).getOrElse(0L)

  final private class LiveCache[K, E, V](
      cache: CaffeineCache[K, Entry[E, V]],
      tick: IO[Unit],
      weigher: Option[V => Int]
  ) extends Cache[K, E, V] {
    // Expiration may remove a Loading placeholder before its loader finishes. Keep its publication
    // generation until completion or explicit mutation; coordinate through Caffeine's per-key compute.
    private val owners = new ConcurrentHashMap[K, Loading[E, V]]()
    private def access[A](action: => A): IO[A] = tick *> IO.delay(action)

    // User code runs before a cache mutation. Caffeine's own weigher is a total field read,
    // so a thrown/negative user weight cannot strand a Loading entry or retire its owner.
    private def loaded(result: Either[E, V]): IO[Loaded[E, V]] = IO.delay {
      val weight = result.fold(_ => 1, value => weigher.fold(1)(_(value)))
      require(weight >= 0, "cache entry weight must be nonnegative")
      Loaded(result, weight)
    }

    private def retire(key: K, loading: Loading[E, V]): IO[Unit] = access {
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

    private def publish(key: K, loading: Loading[E, V], result: Either[E, V]): IO[Unit] =
      loaded(result).flatMap { completed =>
        access {
          cache.asMap().compute(key, (_, entry) => if (owners.remove(key, loading)) completed else entry)
        }.void
      }

    def get(key: K): IO[Option[V]] = access(Option(cache.getIfPresent(key))).flatMap {
      case None                    => IO.pure(None)
      case Some(Loaded(result, _)) => IO.pure(result.toOption)
      case Some(loading: Loading[E, V]) =>
        loading.done.get.map {
          case Completed(Right(value)) => value
          case _                       => None
        }
    }

    def getOrUpdate(key: K)(load: => IO[Either[E, V]]): IO[Either[E, V]] =
      getOrUpdateOption(key)(IO.defer(load).map(_.map(Some(_)))).flatMap {
        case Right(None)        => getOrUpdate(key)(load)
        case Right(Some(value)) => IO.pure(Right(value))
        case Left(error)        => IO.pure(Left(error))
      }

    def getOrUpdateOption(key: K)(load: => IO[Either[E, Option[V]]]): IO[Either[E, Option[V]]] =
      Deferred[IO, Signal[E, V]].map(Loading(_)).flatMap { loading =>
        IO.uncancelable { poll =>
          access(cache.get(key, _ => { owners.put(key, loading); loading })).flatMap {
            case entry if entry eq loading =>
              val abandon = retire(key, loading).guarantee(loading.done.complete(Retry).void)
              poll(IO.defer(load)).onCancel(abandon).attempt.flatMap { attempted =>
                val commit = attempted match {
                  case Right(Left(error))        => publish(key, loading, Left(error))
                  case Right(Right(Some(value))) => publish(key, loading, Right(value))
                  case _                         => retire(key, loading)
                }
                commit.attempt.flatMap {
                  case Left(error) =>
                    retire(key, loading).attempt *>
                      loading.done.complete(Failed(error)) *> IO.raiseError(error)
                  case Right(_) =>
                    attempted match {
                      case Right(result) => loading.done.complete(Completed(result)).as(result)
                      case Left(error)   => loading.done.complete(Failed(error)) *> IO.raiseError(error)
                    }
                }
              }
            case Loaded(result, _) => IO.pure(result.map(Some(_)))
            case current: Loading[E, V] =>
              poll(current.done.get).flatMap {
                case Completed(result) => IO.pure(result)
                case Failed(error)     => IO.raiseError(error)
                case Retry             => poll(getOrUpdateOption(key)(load))
              }
          }
        }
      }

    def put(key: K, value: V): IO[Unit] = loaded(Right(value)).flatMap { completed =>
      access {
        cache.asMap().compute(key, (_, _) => { owners.remove(key); completed })
      }.void
    }
    def invalidate(key: K): IO[Unit] = access {
      cache.asMap().compute(key, (_, _) => { owners.remove(key); null })
    }.void
    def keys: IO[Set[K]] = access { cache.cleanUp(); cache.asMap().keySet().asScala.toSet }
    def size: IO[Long] = access { cache.cleanUp(); cache.estimatedSize() }
    def weight: IO[Long] = access { cache.cleanUp(); weightOf(cache) }
  }
}
