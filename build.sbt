ThisBuild / organization := "io.github.lelik112"
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

lazy val catsEffectVersion = "3.7.1"
lazy val circeVersion = "0.14.10"

lazy val root = (project in file("."))
  .settings(
    name := "cats-utils",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Ymacro-annotations", "-release:21"),
    javacOptions ++= Seq("--release", "21"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "cats-core" % "2.13.0",
      "co.fs2" %% "fs2-core" % "3.12.2",
      "com.github.ben-manes.caffeine" % "caffeine" % "3.2.4",
      "com.github.pureconfig" %% "pureconfig" % "0.17.10",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion % Test,
      "com.chuusai" %% "shapeless" % "2.3.13",
      "com.linecorp.armeria" % "armeria" % "1.38.1",
      "io.micrometer" % "micrometer-core" % "1.16.5",
      "io.micrometer" % "micrometer-registry-prometheus" % "1.16.5",
      "org.scalameta" %% "munit" % "1.0.4" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test,
      "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion % Test
    ),
    Test / parallelExecution := true,
    publish / skip := true
  )
