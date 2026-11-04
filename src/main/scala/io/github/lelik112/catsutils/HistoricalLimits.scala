// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package io.github.lelik112.catsutils

import java.time.{Duration, Instant}

import io.github.lelik112.catsutils.cache.{Cache, CacheMetrics}
import io.github.lelik112.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.circe.generic.JsonCodec

trait HistoricalLimits {
  def apply(id: String, path: String, startTime: Instant): IO[Boolean]
}

object HistoricalLimits {

  private val Expiration = Duration.ofMinutes(1)

  @JsonCodec
  final case class LegacyLimitsConfig(
      olderThan: Duration,
      limit: Int,
      per: Duration
  )

  @JsonCodec
  final case class Config(
      default: LegacyLimitsConfig,
      paths: Map[String, LegacyLimitsConfig]
  ) {
    def apply(path: String): LegacyLimitsConfig = paths.getOrElse(path, default)
  }

  object Config {
    implicit val reader: ConfigReader[Config] = {
      implicit val reader: ConfigReader[LegacyLimitsConfig] = deriveReader
      deriveReader
    }
  }

  def make(config: IO[Config], metrics: CacheMetrics = CacheMetrics.noop): Resource[IO, HistoricalLimits] =
    for {
      observations <- Resource.eval(OrderedConfig.make(config))
      cache <- Cache.expireAfterAccess[(String, String), Nothing, (LegacyLimitsConfig, RateLimits.TokenBucket)](
        "Legacy Rate Limits",
        Expiration,
        metrics = metrics
      )
    } yield new HistoricalLimits {
      override def apply(id: String, path: String, startTime: Instant): IO[Boolean] = observations.use { latest =>
        val currentConfig = latest(path)
        val key = (id, path)
        IO.realTimeInstant.flatMap { now =>
          if (startTime.isBefore(now.minus(currentConfig.olderThan)))
            cache
              .getOrUpdateValue(key) {
                RateLimits.TokenBucket.make(currentConfig.limit, currentConfig.per).map(currentConfig -> _)
              }
              .flatMap {
                case (previousConfig, bucket) if previousConfig == currentConfig => bucket.take
                case _                                                           =>
                  // A bad replacement must not discard an existing, possibly exhausted budget.
                  RateLimits.TokenBucket
                    .make(currentConfig.limit, currentConfig.per)
                    .flatTap(bucket => cache.put(key, currentConfig -> bucket))
                    .flatMap(_.take)
              }
          else IO.pure(true)
        }
      }
    }
}
