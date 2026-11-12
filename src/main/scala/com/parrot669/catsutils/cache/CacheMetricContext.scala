package com.parrot669.catsutils.cache

import cats.Monad
import cats.mtl.Local
import cats.syntax.all._

/** Request-local context supplied by the application. The application must provide a concurrency-safe Local with isolated, nested scopes. */
final class CacheMetricContext[F[_]: Monad] private (local: Local[F, String]) {
  def path: F[String] = local.ask

  def withPath[A](path: String)(effect: F[A]): F[A] =
    local.local(effect)(_ => path)

  def withPathFrom[A](source: F[String])(effect: F[A]): F[A] =
    source.flatMap(path => withPath(path)(effect))
}

object CacheMetricContext {

  /** The application chooses and allocates its context carrier; no global shared Ref is used. */
  def fromLocal[F[_]: Monad](local: Local[F, String]): CacheMetricContext[F] =
    new CacheMetricContext(local)
}
