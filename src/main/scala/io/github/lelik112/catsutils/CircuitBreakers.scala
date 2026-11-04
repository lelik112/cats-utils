// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package io.github.lelik112.catsutils

import java.time.Duration

import scala.util.control.NoStackTrace

import io.github.lelik112.catsutils.CircuitBreakers.{CircuitBreakerOpen, Guarded}
import io.github.lelik112.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import cats.effect.{IO, Outcome, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import com.linecorp.armeria.client.circuitbreaker.{CircuitBreaker, CircuitBreakerBuilder, CircuitBreakerListener}
import io.circe.generic.JsonCodec
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

trait CircuitBreakers {

  /** The default counts successful IO completion, including a returned Left, as success. An effectful predicate can
    * inspect a successful Outcome's value to classify typed business errors independently from raised errors.
    */
  def guard[A](
      onOpen: String => IO[A] = (_: String) => IO.raiseError(CircuitBreakerOpen),
      isSuccess: Outcome[IO, Throwable, A] => IO[Boolean] = (outcome: Outcome[IO, Throwable, A]) =>
        IO.pure(outcome.isSuccess)
  ): Guarded[A]

  /** Preserves the source's default classification for typed failures represented by Left. */
  def guardEither[E, A](
      onOpen: String => IO[Either[E, A]] = (_: String) => IO.raiseError(CircuitBreakerOpen),
      isSuccess: Outcome[IO, Throwable, Either[E, A]] => IO[Boolean] = CircuitBreakers.eitherIsSuccess[E, A] _
  ): Guarded[Either[E, A]] = guard(onOpen, isSuccess)

  /** Records the Resource use exit, including errors and cancellation. Resource.ExitCase carries no return value;
    * use guardEither()(path)(resource.use(...)) to classify typed business errors returned by a Resource use.
    */
  def scoped(
      onOpen: String => IO[Unit] = (_: String) => IO.raiseError(CircuitBreakerOpen),
      isSuccess: Resource.ExitCase => IO[Boolean] = (exit: Resource.ExitCase) =>
        IO.pure(exit == Resource.ExitCase.Succeeded)
  ): String => Resource[IO, Unit]
}

object CircuitBreakers {

  object CircuitBreakerOpen extends RuntimeException("Circuit breaker is open") with NoStackTrace

  private[catsutils] def eitherIsSuccess[E, A](outcome: Outcome[IO, Throwable, Either[E, A]]): IO[Boolean] =
    outcome match {
      case Outcome.Succeeded(value) => value.map(_.isRight)
      case _                        => IO.pure(false)
    }

  @JsonCodec
  final case class CircuitBreakerConfig(
      failureRate: Double,
      minThroughput: Long,
      halfOpenDuration: Duration,
      openDuration: Duration
  )

  @JsonCodec
  final case class Config(
      default: CircuitBreakerConfig,
      paths: Map[String, CircuitBreakerConfig]
  ) {
    def apply(path: String): CircuitBreakerConfig = paths.getOrElse(path, default)
  }

  object Config {
    implicit val reader: ConfigReader[Config] = {
      implicit val reader: ConfigReader[CircuitBreakerConfig] = deriveReader
      deriveReader
    }
  }

  trait Guarded[A] {
    def apply(path: String)(io: IO[A]): IO[A]
  }

  private object Guarded {
    def apply[A](
        breaker: String => IO[CircuitBreaker],
        onOpen: String => IO[A],
        isSuccess: Outcome[IO, Throwable, A] => IO[Boolean]
    ): Guarded[A] = new Guarded[A] {
      override def apply(path: String)(io: IO[A]): IO[A] = breaker(path).flatMap { breaker =>
        // Admission and installation of the outcome observer are atomic with respect to cancellation.
        IO.uncancelable { poll =>
          IO.delay(breaker.tryRequest()).flatMap {
            case true  => poll(io).guaranteeCase(outcome => IO.defer(isSuccess(outcome)).flatMap(record(breaker, _)))
            case false => poll(IO.defer(onOpen(path)))
          }
        }
      }
    }
  }

  final private case class Shard(
      state: Ref[IO, Map[String, (CircuitBreakerConfig, CircuitBreaker)]],
      mutex: Mutex[IO]
  )

  /** The registry remains owned by the caller. */
  def make(
      name: String,
      config: IO[Config],
      registry: PrometheusMeterRegistry,
      shards: Int = Runtime.getRuntime.availableProcessors() * 2
  ): IO[CircuitBreakers] = make(name, config, registry, CircuitBreaker.builder, shards)

  private[catsutils] def make(
      name: String,
      config: IO[Config],
      registry: PrometheusMeterRegistry,
      builder: String => CircuitBreakerBuilder,
      shards: Int
  ): IO[CircuitBreakers] = for {
    _ <- IO.raiseWhen(shards <= 0)(new IllegalArgumentException("shards must be positive"))
    state <- Vector
      .fill(shards) {
        (Ref.of[IO, Map[String, (CircuitBreakerConfig, CircuitBreaker)]](Map.empty), Mutex[IO]).mapN(Shard.apply)
      }
      .sequence
    listener <- IO.delay(CircuitBreakerListener.metricCollecting(registry, name))
  } yield new CircuitBreakers {
    override def guard[A](
        onOpen: String => IO[A],
        isSuccess: Outcome[IO, Throwable, A] => IO[Boolean]
    ): Guarded[A] = Guarded(breaker, onOpen, isSuccess)

    override def scoped(
        onOpen: String => IO[Unit],
        isSuccess: Resource.ExitCase => IO[Boolean]
    ): String => Resource[IO, Unit] = path =>
      Resource
        .makeCaseFull[IO, Option[CircuitBreaker]] { poll =>
          poll(breaker(path)).flatMap { breaker =>
            IO.delay(breaker.tryRequest()).flatMap {
              case true  => IO.pure(Some(breaker))
              case false => poll(IO.defer(onOpen(path))).as(None)
            }
          }
        } { (admitted, exit) =>
          admitted.traverse_(breaker => IO.defer(isSuccess(exit)).flatMap(record(breaker, _)))
        }
        .void

    private def breaker(path: String): IO[CircuitBreaker] = config.map(_(path)).flatMap { currentConfig =>
      val shard = state(Math.floorMod(path.hashCode, shards))
      // Building a breaker installs listeners, so keep that side effect outside Ref.modify's retryable function.
      shard.mutex.lock.surround {
        IO.uncancelable { _ =>
          shard.state.get.flatMap { breakers =>
            breakers.get(path) match {
              case Some((`currentConfig`, breaker)) => IO.pure(breaker)
              case _ =>
                IO.delay {
                  builder(path)
                    .failureRateThreshold(currentConfig.failureRate)
                    .minimumRequestThreshold(currentConfig.minThroughput)
                    .circuitOpenWindow(currentConfig.openDuration)
                    .trialRequestInterval(currentConfig.halfOpenDuration)
                    .listener(listener)
                    .build()
                }.flatTap(breaker => shard.state.set(breakers.updated(path, (currentConfig, breaker))))
            }
          }
        }
      }
    }
  }

  private def record(breaker: CircuitBreaker, successful: Boolean): IO[Unit] = IO.delay {
    if (successful) breaker.onSuccess()
    else breaker.onFailure()
  }
}
