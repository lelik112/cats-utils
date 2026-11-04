// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package io.github.lelik112.catsutils

import java.time.Duration

import io.github.lelik112.catsutils.cache.{Cache, CacheMetrics}
import io.github.lelik112.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.circe.generic.JsonCodec

trait RateLimits {
  def apply(id: String, path: String): IO[Boolean]
}

object RateLimits {

  private val Expiration = Duration.ofMinutes(1)

  @JsonCodec
  final case class RateLimitsConfig(
      limit: Int,
      per: Duration,
      users: Map[String, RateLimitsConfig] = Map.empty
  )

  object RateLimitsConfig {
    implicit val reader: ConfigReader[RateLimitsConfig] = deriveReader
  }

  @JsonCodec
  final case class Config(
      default: RateLimitsConfig,
      paths: Map[String, RateLimitsConfig]
  ) {
    def apply(id: String, path: String): RateLimitsConfig = {
      val config = paths.getOrElse(path, default)
      config.users.getOrElse(id, config)
    }
  }

  object Config {
    implicit val reader: ConfigReader[Config] = deriveReader
  }

  def make(config: IO[Config], metrics: CacheMetrics = CacheMetrics.noop): Resource[IO, RateLimits] =
    for {
      observations <- Resource.eval(OrderedConfig.make(config))
      cache <- Cache.expireAfterAccess[(String, String), Nothing, (RateLimitsConfig, TokenBucket)](
        "Rate Limits",
        Expiration,
        metrics = metrics
      )
    } yield new RateLimits {
      override def apply(id: String, path: String): IO[Boolean] = observations.use { latest =>
        val currentConfig = latest(id, path)
        val key = (id, path)
        cache
          .getOrUpdateValue(key) {
            TokenBucket.make(currentConfig.limit, currentConfig.per).map(currentConfig -> _)
          }
          .flatMap {
            case (previousConfig, bucket) if previousConfig == currentConfig => bucket.take
            case _                                                           =>
              // Validate/allocate before replacing: a failed configuration leaves the old bucket intact.
              TokenBucket
                .make(currentConfig.limit, currentConfig.per)
                .flatTap(bucket => cache.put(key, currentConfig -> bucket))
                .flatMap(_.take)
          }
      }
    }

  trait TokenBucket {
    def take: IO[Boolean]
  }

  // Token-bucket logic adapted from http4s Throttle, Copyright 2014 http4s.org, under Apache 2.0.
  // Modified for this utility's IO/Boolean API, atomic state updates and refill behavior.
  // Upstream attribution and terms: PROVENANCE.md and third-party-licenses/http4s-LICENSE.
  // Original source acknowledgment:
  // https://github.com/http4s/http4s/blob/series/0.23/server/shared/src/main/scala/org/http4s/server/middleware/Throttle.scala#L54
  object TokenBucket {
    // The source accepts a zero refill interval; only negative intervals are rejected.
    def make(capacity: Int, refillEvery: Duration): IO[TokenBucket] = for {
      _ <- IO.raiseWhen(capacity <= 0)(new IllegalArgumentException("capacity must be positive"))
      _ <- IO.raiseWhen(refillEvery.isNegative)(new IllegalArgumentException("refillEvery must be positive"))
      now <- IO.monotonic.map(_.toNanos)
      counter <- Ref.of[IO, (Double, Long)](capacity.toDouble -> now)
    } yield new TokenBucket {
      override val take: IO[Boolean] = IO.monotonic.map(_.toNanos).flatMap { now =>
        counter.modify {
          case (tokens, time) if now > time =>
            val diff = Math.max(0, now - time)
            val add = diff.toDouble / refillEvery.toNanos.toDouble
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
