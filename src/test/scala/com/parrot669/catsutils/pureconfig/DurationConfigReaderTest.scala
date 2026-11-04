package com.parrot669.catsutils

import java.time.Duration

import com.parrot669.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.ConfigSource
import _root_.pureconfig.generic.semiauto.deriveReader
import munit.FunSuite

/** New config-boundary checks for the source's Scala-duration to Java-duration conversion. */
final class DurationConfigReaderTest extends FunSuite {
  private def read(value: String) =
    ConfigSource.string(s"value = $value").at("value").load[Duration]

  test("finite configuration values retain nanosecond precision and sign") {
    assertEquals(read("1500 ns"), Right(Duration.ofNanos(1500L)))
    assertEquals(read("1.5 seconds"), Right(Duration.ofMillis(1500L)))
    assertEquals(read("0 ns"), Right(Duration.ZERO))
    assertEquals(read("-2 ms"), Right(Duration.ofMillis(-2L)))
  }

  test("non-finite configuration values retain the source infinity sentinel") {
    assertEquals(read("Inf"), Right(Duration.ofNanos(Long.MaxValue)))
    assertEquals(read("MinusInf"), Right(Duration.ofNanos(Long.MaxValue)))
    assertEquals(read("Undefined"), Right(Duration.ofNanos(Long.MaxValue)))
  }

  test("malformed duration values produce reader failures") {
    assert(read("not-a-duration").isLeft)
    assert(read("[1, 2]").isLeft)
  }

  test("the implicit reader participates in derived configuration readers") {
    import DurationConfigReaderTest.Settings
    implicit val reader: ConfigReader[Settings] = deriveReader[Settings]
    assertEquals(
      ConfigSource.string("interval = 250 ms").load[Settings],
      Right(Settings(Duration.ofMillis(250L)))
    )
  }
}

object DurationConfigReaderTest {
  final case class Settings(interval: Duration)
}
