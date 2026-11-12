package com.parrot669.catsutils

import cats.effect.std.Mutex
import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._

/** Orders successful configuration observations by the start of their reads, not their completion.
  *
  * The source effect runs outside the mutex, so a stalled read cannot block newer requests. The newest
  * accepted full snapshot wins, even when its value equals the previous snapshot. These local tickets
  * are not external configuration revisions: a source requiring true revision ordering must expose
  * those revisions explicitly rather than relying on arbitrary effect completion order.
  */
private[catsutils] final class OrderedConfig[F[_]: Async, C] private (
  source:   F[C],
  tickets:  Ref[F, BigInt],
  accepted: Ref[F, Option[(BigInt, C)]],
  mutex:    Mutex[F]
) {

  /** The callback is restricted to the limiters' short internal bucket operations. Once admitted,
    * publication, bucket replacement, and token consumption complete together under cancellation
    * masking. Configuration reads and waiting for this gate remain cancelable.
    */
  def use[A](f: C => F[A]): F[A] = for {
    ticket   <- tickets.getAndUpdate(_ + 1)
    snapshot <- source
    result   <- mutex.lock.surround {
                  Async[F].uncancelable { _ =>
                    accepted
                      .modify {
                        case previous @ Some((sequence, value)) if sequence >= ticket => (previous, value)
                        case _                                                        => (Some(ticket -> snapshot), snapshot)
                      }
                      .flatMap(value => Async[F].defer(f(value)))
                  }
                }
  } yield result
}

private[catsutils] object OrderedConfig {
  def make[F[_]: Async, C](source: F[C]): F[OrderedConfig[F, C]] =
    (Ref.of[F, BigInt](BigInt(0)), Ref.of[F, Option[(BigInt, C)]](None), Mutex[F])
      .mapN(new OrderedConfig(source, _, _, _))
}
