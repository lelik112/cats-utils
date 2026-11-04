// Adapted from owner-supplied utilities attributed to Bobylev (Бобылев); see PROVENANCE.md.
package com.parrot669.catsutils.cache

import cats.effect.IO

private[cache] trait SpecificCacheMetrics {
  def hit(locked: Boolean): IO[Unit]
  def load: IO[Unit]
}

private[cache] object SpecificCacheMetrics {
  def apply(specificName: String, cacheName: String, metrics: CacheMetrics = CacheMetrics.noop): SpecificCacheMetrics =
    new SpecificCacheMetrics {
      def hit(locked: Boolean): IO[Unit] = metrics.path.flatMap { path =>
        metrics.increment(
          s"${specificName}_hits_total",
          Map("name" -> cacheName, "locked" -> locked.toString, "path" -> path)
        )
      }
      def load: IO[Unit]                 = metrics.path.flatMap { path =>
        metrics.increment(s"${specificName}_loads_total", Map("name" -> cacheName, "path" -> path))
      }
    }
}
