// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils

import java.time.{Duration, Instant}

import cats.Defer
import cats.effect.syntax.all._
import cats.effect.{Resource, Sync, Temporal}
import cats.syntax.all._

import com.parrot669.catsutils.cache.{Cache, CacheMetrics}
import com.parrot669.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import io.circe.generic.JsonCodec

trait HistoricalLimits[F[_]] {
  def apply(id: String, path: String, startTime: Instant): F[Boolean]
}

object HistoricalLimits {

  private val Expiration = Duration.ofMinutes(1)

  @JsonCodec
  final case class LegacyLimitsConfig(
    olderThan: Duration,
    limit:     Int,
    per:       Duration
  )

  @JsonCodec
  final case class Config(
    default: LegacyLimitsConfig,
    paths:   Map[String, LegacyLimitsConfig]
  ) {
    def apply(path: String): LegacyLimitsConfig = paths.getOrElse(path, default)
  }

  object Config {
    implicit val reader: ConfigReader[Config] = {
      implicit val reader: ConfigReader[LegacyLimitsConfig] = deriveReader
      deriveReader
    }
  }

  def make[F[_]: Sync: Temporal](config: F[Config]): Resource[F, HistoricalLimits[F]] =
    make(config, CacheMetrics.noop[F](Temporal[F]))

  def make[F[_]: Sync: Temporal](config: F[Config], metrics: CacheMetrics[F]): Resource[F, HistoricalLimits[F]] =
    makeImpl(config, metrics, Sync[F])(Temporal[F])

  private def makeImpl[F[_]](config: F[Config], metrics: CacheMetrics[F], S: Sync[F])(implicit
    F: Temporal[F]
  ): Resource[F, HistoricalLimits[F]] = {
    implicit val deferred: Defer[F] = S
    for {
      observations <- Resource.eval(OrderedConfig.make(config))
      cache        <- Cache.expireAfterAccess[F, (String, String), Nothing, (LegacyLimitsConfig, RateLimits.TokenBucket[F])](
                        "Legacy Rate Limits",
                        Expiration,
                        metrics = metrics
                      )(S, F)
    } yield new HistoricalLimits[F] {
      override def apply(id: String, path: String, startTime: Instant): F[Boolean] = observations.use { latest =>
        val currentConfig = latest(path)
        val key           = (id, path)
        F.realTimeInstant.flatMap { now =>
          if (startTime.isBefore(now.minus(currentConfig.olderThan)))
            cache
              .getOrUpdateValue(key) {
                RateLimits.TokenBucket.make[F](currentConfig.limit, currentConfig.per).map(currentConfig -> _)
              }
              .flatMap {
                case (previousConfig, bucket) if previousConfig == currentConfig => bucket.take
                case _                                                           =>
                  // A bad replacement must not discard an existing, possibly exhausted budget.
                  RateLimits.TokenBucket
                    .make[F](currentConfig.limit, currentConfig.per)
                    .flatTap(bucket => cache.put(key, currentConfig -> bucket))
                    .flatMap(_.take)
              }
          else F.pure(true)
        }
      }
    }
  }
}
