// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils

import java.time.Duration
import scala.util.control.NoStackTrace

import cats.effect.std.Mutex
import cats.effect.syntax.all._
import cats.effect.{Async, Outcome, Ref, Resource}
import cats.syntax.all._

import com.parrot669.catsutils.CircuitBreakers.{CircuitBreakerOpen, Guarded}
import com.parrot669.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import com.linecorp.armeria.client.circuitbreaker.{CircuitBreaker, CircuitBreakerBuilder, CircuitBreakerListener}
import io.circe.generic.JsonCodec
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

abstract class CircuitBreakers[F[_]: cats.MonadThrow] {

  /** The default counts successful effect completion, including a returned Left, as success. An effectful predicate can
    * inspect a successful Outcome's value to classify typed business errors independently from raised errors.
    */
  def guard[A](
    onOpen: String => F[A] = (_: String) => cats.MonadThrow[F].raiseError[A](CircuitBreakerOpen),
    isSuccess: Outcome[F, Throwable, A] => F[Boolean] = (outcome: Outcome[F, Throwable, A]) =>
      cats.MonadThrow[F].pure(outcome.isSuccess)
  ): Guarded[F, A]

  /** Preserves the source's default classification for typed failures represented by Left. */
  def guardEither[E, A](
    onOpen:    String => F[Either[E, A]] = (_: String) => cats.MonadThrow[F].raiseError[Either[E, A]](CircuitBreakerOpen),
    isSuccess: Outcome[F, Throwable, Either[E, A]] => F[Boolean] = CircuitBreakers.eitherIsSuccess[F, E, A] _
  ): Guarded[F, Either[E, A]] = guard(onOpen, isSuccess)

  /** Records the Resource use exit, including errors and cancellation. Resource.ExitCase carries no return value;
    * use guardEither()(path)(resource.use(...)) to classify typed business errors returned by a Resource use.
    */
  def scoped(
    onOpen: String => F[Unit] = (_: String) => cats.MonadThrow[F].raiseError[Unit](CircuitBreakerOpen),
    isSuccess: Resource.ExitCase => F[Boolean] = (exit: Resource.ExitCase) =>
      cats.MonadThrow[F].pure(exit == Resource.ExitCase.Succeeded)
  ): String => Resource[F, Unit]
}

object CircuitBreakers {

  object CircuitBreakerOpen extends RuntimeException("Circuit breaker is open") with NoStackTrace

  private[catsutils] def eitherIsSuccess[F[_]: cats.Applicative, E, A](
    outcome: Outcome[F, Throwable, Either[E, A]]
  ): F[Boolean] =
    outcome match {
      case Outcome.Succeeded(value) => value.map(_.isRight)
      case _                        => cats.Applicative[F].pure(false)
    }

  @JsonCodec
  final case class CircuitBreakerConfig(
    failureRate:      Double,
    minThroughput:    Long,
    halfOpenDuration: Duration,
    openDuration:     Duration
  )

  @JsonCodec
  final case class Config(
    default: CircuitBreakerConfig,
    paths:   Map[String, CircuitBreakerConfig]
  ) {
    def apply(path: String): CircuitBreakerConfig = paths.getOrElse(path, default)
  }

  object Config {
    implicit val reader: ConfigReader[Config] = {
      implicit val reader: ConfigReader[CircuitBreakerConfig] = deriveReader
      deriveReader
    }
  }

  trait Guarded[F[_], A] {
    def apply(path: String)(effect: F[A]): F[A]
  }

  private object Guarded {
    def apply[F[_]: Async, A](
      breaker:   String => F[CircuitBreaker],
      onOpen:    String => F[A],
      isSuccess: Outcome[F, Throwable, A] => F[Boolean]
    ): Guarded[F, A] = new Guarded[F, A] {
      override def apply(path: String)(effect: F[A]): F[A] = breaker(path).flatMap { breaker =>
        // Admission and installation of the outcome observer are atomic with respect to cancellation.
        Async[F].uncancelable { poll =>
          Async[F].delay(breaker.tryRequest()).flatMap {
            case true  =>
              poll(effect).guaranteeCase(outcome => Async[F].defer(isSuccess(outcome)).flatMap(record(breaker, _)))
            case false => poll(Async[F].defer(onOpen(path)))
          }
        }
      }
    }
  }

  private final case class Shard[F[_]](
    state: Ref[F, Map[String, (CircuitBreakerConfig, CircuitBreaker)]],
    mutex: Mutex[F]
  )

  /** The registry remains owned by the caller. */
  def make[F[_]: Async](
    name:     String,
    config:   F[Config],
    registry: PrometheusMeterRegistry,
    shards:   Int = Runtime.getRuntime.availableProcessors() * 2
  ): F[CircuitBreakers[F]] = make(name, config, registry, CircuitBreaker.builder, shards)

  private[catsutils] def make[F[_]: Async](
    name:     String,
    config:   F[Config],
    registry: PrometheusMeterRegistry,
    builder:  String => CircuitBreakerBuilder,
    shards:   Int
  ): F[CircuitBreakers[F]] = for {
    _        <- Async[F].raiseWhen(shards <= 0)(new IllegalArgumentException("shards must be positive"))
    state    <- Vector
                  .fill(shards) {
                    (Ref.of[F, Map[String, (CircuitBreakerConfig, CircuitBreaker)]](Map.empty), Mutex[F])
                      .mapN(Shard.apply)
                  }
                  .sequence
    listener <- Async[F].delay(CircuitBreakerListener.metricCollecting(registry, name))
  } yield new CircuitBreakers[F] {
    override def guard[A](
      onOpen:    String => F[A],
      isSuccess: Outcome[F, Throwable, A] => F[Boolean]
    ): Guarded[F, A] = Guarded(breaker, onOpen, isSuccess)

    override def scoped(
      onOpen:    String => F[Unit],
      isSuccess: Resource.ExitCase => F[Boolean]
    ): String => Resource[F, Unit] = path =>
      Resource
        .makeCaseFull[F, Option[CircuitBreaker]] { poll =>
          poll(breaker(path)).flatMap { breaker =>
            Async[F].delay(breaker.tryRequest()).flatMap {
              case true  => Async[F].pure(Some(breaker))
              case false => poll(Async[F].defer(onOpen(path))).as(None)
            }
          }
        } { (admitted, exit) =>
          admitted.traverse_(breaker => Async[F].defer(isSuccess(exit)).flatMap(record(breaker, _)))
        }
        .void

    private def breaker(path: String): F[CircuitBreaker] = config.map(_(path)).flatMap { currentConfig =>
      val shard = state(Math.floorMod(path.hashCode, shards))
      // Building a breaker installs listeners, so keep that side effect outside Ref.modify's retryable function.
      shard.mutex.lock.surround {
        Async[F].uncancelable { _ =>
          shard.state.get.flatMap { breakers =>
            breakers.get(path) match {
              case Some((`currentConfig`, breaker)) => Async[F].pure(breaker)
              case _                                =>
                Async[F]
                  .delay {
                    builder(path)
                      .failureRateThreshold(currentConfig.failureRate)
                      .minimumRequestThreshold(currentConfig.minThroughput)
                      .circuitOpenWindow(currentConfig.openDuration)
                      .trialRequestInterval(currentConfig.halfOpenDuration)
                      .listener(listener)
                      .build()
                  }
                  .flatTap(breaker => shard.state.set(breakers.updated(path, (currentConfig, breaker))))
            }
          }
        }
      }
    }
  }

  private def record[F[_]: cats.effect.Sync](breaker: CircuitBreaker, successful: Boolean): F[Unit] =
    cats.effect.Sync[F].delay {
      if (successful) breaker.onSuccess()
      else breaker.onFailure()
    }
}
