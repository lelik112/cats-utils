// Adapted from owner-supplied utilities attributed to Bobylev (Бобылев); see PROVENANCE.md.
package com.parrot669.catsutils.cache

import cats.FlatMap
import cats.syntax.all._

private[cache] trait SpecificCacheMetrics[F[_]] {
  def hit(locked: Boolean): F[Unit]
  def load: F[Unit]
}

private[cache] object SpecificCacheMetrics {
  def apply[F[_]: FlatMap](
    specificName: String,
    cacheName:    String,
    metrics:      CacheMetrics[F]
  ): SpecificCacheMetrics[F] =
    new SpecificCacheMetrics[F] {
      def hit(locked: Boolean): F[Unit] = metrics.path.flatMap { path =>
        metrics.increment(
          s"${specificName}_hits_total",
          Map("name" -> cacheName, "locked" -> locked.toString, "path" -> path)
        )
      }
      def load: F[Unit]                 = metrics.path.flatMap { path =>
        metrics.increment(s"${specificName}_loads_total", Map("name" -> cacheName, "path" -> path))
      }
    }
}
