// Provenance: DurationConfigReader excerpt from the supplied Bobylev (Бобылев) utilities; see PROVENANCE.md.
// Source attribution and adaptation notes: see PROVENANCE.md.
package io.github.lelik112.catsutils

import java.time.Duration
import scala.concurrent.duration.FiniteDuration

import _root_.pureconfig.ConfigReader

// Other source readers depend on integrations outside the supplied module; they are not recreated here.
object pureconfig {
  implicit val DurationConfigReader: ConfigReader[Duration] =
    ConfigReader[scala.concurrent.duration.Duration].map {
      case duration: FiniteDuration => Duration.ofNanos(duration.toNanos)
      // Retains the source's sentinel for every non-finite Scala duration, including negative infinity.
      case _ => Duration.ofNanos(Long.MaxValue)
    }
}
