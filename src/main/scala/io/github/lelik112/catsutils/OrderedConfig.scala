package io.github.lelik112.catsutils

import cats.effect.{IO, Ref}
import cats.effect.std.Mutex
import cats.syntax.all._

/** Orders successful configuration observations by the start of their reads, not their completion.
  *
  * The source IO runs outside the mutex, so a stalled read cannot block newer requests. The newest
  * accepted full snapshot wins, even when its value equals the previous snapshot. These local tickets
  * are not external configuration revisions: a source requiring true revision ordering must expose
  * those revisions explicitly rather than relying on arbitrary IO completion order.
  */
final private[catsutils] class OrderedConfig[C] private (
    source: IO[C],
    tickets: Ref[IO, BigInt],
    accepted: Ref[IO, Option[(BigInt, C)]],
    mutex: Mutex[IO]
) {

  /** The callback is restricted to the limiters' short internal bucket operations. Once admitted,
    * publication, bucket replacement, and token consumption complete together under cancellation
    * masking. Configuration reads and waiting for this gate remain cancelable.
    */
  def use[A](f: C => IO[A]): IO[A] = for {
    ticket <- tickets.getAndUpdate(_ + 1)
    snapshot <- source
    result <- mutex.lock.surround {
      IO.uncancelable { _ =>
        accepted
          .modify {
            case previous @ Some((sequence, value)) if sequence >= ticket => (previous, value)
            case _                                                        => (Some(ticket -> snapshot), snapshot)
          }
          .flatMap(value => IO.defer(f(value)))
      }
    }
  } yield result
}

private[catsutils] object OrderedConfig {
  def make[C](source: IO[C]): IO[OrderedConfig[C]] =
    (Ref.of[IO, BigInt](BigInt(0)), Ref.of[IO, Option[(BigInt, C)]](None), Mutex[IO])
      .mapN(new OrderedConfig(source, _, _, _))
}
