package com.parrot669.catsutils.cache

import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

import io.micrometer.core.instrument.{Gauge, MeterRegistry, Tag}

/** Explicit metric destination and request-path context. The caller owns the registry. */
trait CacheMetrics[F[_]] {
  def path: F[String]
  def increment(name: String, tags: Map[String, String]): F[Unit]
  def gauge(name: String, tags: Map[String, String], value: Double): F[Unit]
}

object CacheMetrics {
  private type GaugeValues = ConcurrentHashMap[(String, Map[String, String]), AtomicReference[java.lang.Double]]
  private val registryGauges                                  = new WeakHashMap[MeterRegistry, GaugeValues]()
  private def gaugesFor(registry: MeterRegistry): GaugeValues = registryGauges.synchronized {
    val existing = registryGauges.get(registry)
    if (existing != null) existing
    else {
      val created = new GaugeValues()
      registryGauges.put(registry, created)
      created
    }
  }

  def noop[F[_]: cats.Applicative]: CacheMetrics[F] = new CacheMetrics[F] {
    def path: F[String] = cats.Applicative[F].pure("")
    def increment(name: String, tags: Map[String, String]): F[Unit] = cats.Applicative[F].unit
    def gauge(name:     String, tags: Map[String, String], value: Double): F[Unit] = cats.Applicative[F].unit
  }

  def micrometer[F[_]: cats.effect.Sync](registry: MeterRegistry, context: CacheMetricContext[F]): CacheMetrics[F] =
    new CacheMetrics[F] {
      private def gauges = gaugesFor(registry)

      def path: F[String] = context.path

      private def labels(tags: Map[String, String]): java.lang.Iterable[Tag] =
        tags.toList.sortBy(_._1).map { case (key, value) => Tag.of(key, value) }.asJava

      def increment(name: String, tags: Map[String, String]): F[Unit] =
        cats.effect.Sync[F].delay(registry.counter(name, labels(tags)).increment())

      def gauge(name: String, tags: Map[String, String], value: Double): F[Unit] = cats.effect.Sync[F].delay {
        val reference = gauges.computeIfAbsent(
          (name, tags),
          _ => {
            val value = new AtomicReference[java.lang.Double](0.0)
            Gauge
              .builder(name, value, (ref: AtomicReference[java.lang.Double]) => ref.get().doubleValue())
              .tags(labels(tags))
              .strongReference(true)
              .register(registry)
            value
          }
        )
        reference.set(value)
      }
    }
}
