package io.github.lelik112.catsutils.cache

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.WeakHashMap
import scala.jdk.CollectionConverters._

import cats.effect.IO
import io.micrometer.core.instrument.{Gauge, MeterRegistry, Tag}

/** Explicit metric destination and request-path context. The caller owns the registry. */
trait CacheMetrics {
  def path: IO[String]
  def increment(name: String, tags: Map[String, String]): IO[Unit]
  def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit]
}

object CacheMetrics {
  private type GaugeValues = ConcurrentHashMap[(String, Map[String, String]), AtomicReference[java.lang.Double]]
  private val registryGauges = new WeakHashMap[MeterRegistry, GaugeValues]()
  private def gaugesFor(registry: MeterRegistry): GaugeValues = registryGauges.synchronized {
    val existing = registryGauges.get(registry)
    if (existing != null) existing
    else {
      val created = new GaugeValues()
      registryGauges.put(registry, created)
      created
    }
  }

  val noop: CacheMetrics = new CacheMetrics {
    def path: IO[String] = IO.pure("")
    def increment(name: String, tags: Map[String, String]): IO[Unit] = IO.unit
    def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] = IO.unit
  }

  def micrometer(registry: MeterRegistry, context: CacheMetricContext): CacheMetrics = new CacheMetrics {
    private val gauges = gaugesFor(registry)

    def path: IO[String] = context.path

    private def labels(tags: Map[String, String]): java.lang.Iterable[Tag] =
      tags.toList.sortBy(_._1).map { case (key, value) => Tag.of(key, value) }.asJava

    def increment(name: String, tags: Map[String, String]): IO[Unit] =
      IO.delay(registry.counter(name, labels(tags)).increment())

    def gauge(name: String, tags: Map[String, String], value: Double): IO[Unit] = IO.delay {
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
