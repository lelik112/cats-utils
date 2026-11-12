// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils

import java.time.Duration

import cats.effect.syntax.all._
import cats.effect.{Async, Ref, Resource, Temporal}
import cats.syntax.all._

import com.parrot669.catsutils.cache.{Cache, CacheMetrics}
import com.parrot669.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import io.circe.generic.JsonCodec

trait RateLimits[F[_]] {
  def apply(id: String, path: String): F[Boolean]
}

object RateLimits {

  private val Expiration = Duration.ofMinutes(1)

  @JsonCodec
  final case class RateLimitsConfig(
    limit: Int,
    per:   Duration,
    users: Map[String, RateLimitsConfig] = Map.empty
  )

  object RateLimitsConfig {
    implicit val reader: ConfigReader[RateLimitsConfig] = deriveReader
  }

  @JsonCodec
  final case class Config(
    default: RateLimitsConfig,
    paths:   Map[String, RateLimitsConfig]
  ) {
    def apply(id: String, path: String): RateLimitsConfig = {
      val config = paths.getOrElse(path, default)
      config.users.getOrElse(id, config)
    }
  }

  object Config {
    implicit val reader: ConfigReader[Config] = deriveReader
  }

  def make[F[_]: Async](config: F[Config]): Resource[F, RateLimits[F]] =
    make(config, CacheMetrics.noop[F])

  def make[F[_]: Async](config: F[Config], metrics: CacheMetrics[F]): Resource[F, RateLimits[F]] =
    for {
      observations <- Resource.eval(OrderedConfig.make(config))
      cache        <- Cache.expireAfterAccess[F, (String, String), Nothing, (RateLimitsConfig, TokenBucket[F])](
                        "Rate Limits",
                        Expiration,
                        metrics = metrics
                      )
    } yield new RateLimits[F] {
      override def apply(id: String, path: String): F[Boolean] = observations.use { latest =>
        val currentConfig = latest(id, path)
        val key           = (id, path)
        cache
          .getOrUpdateValue(key) {
            TokenBucket.make[F](currentConfig.limit, currentConfig.per).map(currentConfig -> _)
          }
          .flatMap {
            case (previousConfig, bucket) if previousConfig == currentConfig => bucket.take
            case _                                                           =>
              // Validate/allocate before replacing: a failed configuration leaves the old bucket intact.
              TokenBucket
                .make[F](currentConfig.limit, currentConfig.per)
                .flatTap(bucket => cache.put(key, currentConfig -> bucket))
                .flatMap(_.take)
          }
      }
    }

  trait TokenBucket[F[_]] {
    def take: F[Boolean]
  }

  // Token-bucket logic adapted from http4s Throttle, Copyright 2014 http4s.org, under Apache 2.0.
  // Modified for this utility's F/Boolean API, atomic state updates and refill behavior.
  // Upstream attribution and terms: PROVENANCE.md and third-party-licenses/http4s-LICENSE.
  // Original source acknowledgment:
  // https://github.com/http4s/http4s/blob/series/0.23/server/shared/src/main/scala/org/http4s/server/middleware/Throttle.scala#L54
  object TokenBucket {
    // The source accepts a zero refill interval; only negative intervals are rejected.
    def make[F[_]: Temporal](capacity: Int, refillEvery: Duration): F[TokenBucket[F]] = for {
      _       <- Temporal[F].raiseWhen(capacity <= 0)(new IllegalArgumentException("capacity must be positive"))
      _       <- Temporal[F].raiseWhen(refillEvery.isNegative)(new IllegalArgumentException("refillEvery must be positive"))
      now     <- Temporal[F].monotonic.map(_.toNanos)
      counter <- Ref.of[F, (Double, Long)](capacity.toDouble -> now)
    } yield new TokenBucket[F] {
      override val take: F[Boolean] = Temporal[F].monotonic.map(_.toNanos).flatMap { now =>
        counter.modify {
          case (tokens, time) if now > time =>
            val diff  = Math.max(0, now - time)
            val add   = diff.toDouble / refillEvery.toNanos.toDouble
            val total = Math.min(tokens + add, capacity.toDouble)
            if (total >= 1) ((total - 1, now), true)
            else ((total, now), false)

          case (tokens, time) =>
            if (tokens >= 1) ((tokens - 1, time), true)
            else ((tokens, time), false)
        }
      }
    }
  }
}
