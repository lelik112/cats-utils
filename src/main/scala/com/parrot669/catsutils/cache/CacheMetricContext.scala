package com.parrot669.catsutils.cache

import cats.effect.{IO, IOLocal}

/** Allocated explicitly: child fibers inherit the path; nested scopes restore their caller's path. */
final class CacheMetricContext private (local: IOLocal[String]) {
  def path: IO[String] = local.get

  def withPath[A](path: String)(effect: IO[A]): IO[A] =
    IO.uncancelable { poll =>
      local.get.flatMap(previous => local.set(path) *> poll(effect).guarantee(local.set(previous)))
    }

  def withPathFrom[A](source: IO[String])(effect: IO[A]): IO[A] =
    source.flatMap(path => withPath(path)(effect))
}

object CacheMetricContext {
  def create: IO[CacheMetricContext] = IOLocal("").map(new CacheMetricContext(_))
}
