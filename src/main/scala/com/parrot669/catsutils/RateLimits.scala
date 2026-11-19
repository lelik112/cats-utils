// Adapted from the owner-supplied utility attributed to Bobylev; see PROVENANCE.md.
package com.parrot669.catsutils

import java.time.Duration

import cats.effect.{Clock, Ref, Resource, Sync, Temporal}
import cats.syntax.all._
import cats.{Defer, MonadThrow}

import com.parrot669.catsutils.pureconfig.DurationConfigReader

import _root_.pureconfig.ConfigReader
import _root_.pureconfig.generic.semiauto.deriveReader
import io.circe.generic.JsonCodec

trait RateLimits[F[_]] {
  def apply(id: String, path: String): F[RateLimits.Decision]
}

object RateLimits {
  sealed trait Decision {
    final def isAllowed: Boolean = this == Allowed
  }

  case object Allowed extends Decision

  /** Time until one token is available if no other request consumes it first. */
  final case class Exhausted private[catsutils] (retryAfter: Duration) extends Decision {
    def retryAfterSeconds: Long = retryAfter.getSeconds + (if (retryAfter.getNano == 0) 0L else 1L)
  }

  /** All slots retain debt. No reliable retry time is promised for a new identity. */
  case object CapacityRejected extends Decision

  @JsonCodec
  final case class RateLimitsConfig(
    limit: Int,
    per:   Duration,
    users: Map[String, RateLimitsConfig] = Map.empty
  )

  object RateLimitsConfig {
    implicit val reader: ConfigReader[RateLimitsConfig] = deriveReader
  }

  @JsonCodec
  final case class Config(
    default: RateLimitsConfig,
    paths:   Map[String, RateLimitsConfig]
  ) {
    def apply(id: String, path: String): RateLimitsConfig = {
      val config = paths.getOrElse(path, default)
      config.users.getOrElse(id, config)
    }
  }

  object Config {
    implicit val reader: ConfigReader[Config] = deriveReader
  }

  private type Key = (String, String)
  private final case class Entry(rule: TokenBucket.Rule, state: TokenBucket.State)

  private def validate(config: Config): Either[IllegalArgumentException, Config] = {
    def rule(value: RateLimitsConfig): Either[IllegalArgumentException, Unit] =
      TokenBucket.rule(value.limit, value.per).void *> value.users.values.toList.traverse_(rule)
    (rule(config.default) *> config.paths.values.toList.traverse_(rule)).as(config)
  }

  /** maxKeys is fixed for this Resource's lifetime; there is no implicit production capacity.
    * Only fully recovered entries may retire, lazily when a new key needs room.
    * An effective rule change explicitly resets that key's bucket, keeping its existing slot.
    */
  def make[F[_]: Sync: Temporal](config: F[Config], maxKeys: Int): Resource[F, RateLimits[F]] =
    makeImpl(config, maxKeys, Sync[F])(Temporal[F])

  private def makeImpl[F[_]](config: F[Config], maxKeys: Int, S: Sync[F])(implicit
    F: Temporal[F]
  ): Resource[F, RateLimits[F]] = {
    implicit val deferred: Defer[F] = S
    for {
      _            <- Resource.eval(F.raiseWhen(maxKeys <= 0)(new IllegalArgumentException("maxKeys must be positive")))
      observations <- Resource.eval(OrderedConfig.make(config))
      storage      <- Resource.make(Ref.of[F, Option[Map[Key, Entry]]](Some(Map.empty)))(_.set(None))
    } yield new RateLimits[F] {
      override def apply(id: String, path: String): F[Decision] = observations.use { latest =>
        for {
          valid    <- F.fromEither(validate(latest))
          current   = valid(id, path)
          rule     <- F.fromEither(TokenBucket.rule(current.limit, current.per))
          now      <- TokenBucket.now[F]
          result   <- storage.modify {
                        case None          => None -> Left(new IllegalStateException("RateLimits resource is closed"))
                        case Some(entries) =>
                          val key      = id -> path
                          val retained =
                            if (!entries.contains(key) && entries.size >= maxKeys)
                              entries.filterNot { case (_, entry) => TokenBucket.remaining(entry.state, now) == 0L }
                            else entries
                          retained.get(key) match {
                            case None if retained.size >= maxKeys => Some(retained) -> Right(CapacityRejected: Decision)
                            case previous                         =>
                              val state               = previous match {
                                case Some(entry) if entry.rule == rule => entry.state
                                case _                                 => TokenBucket.State(0L, now)
                              }
                              val (updated, decision) = TokenBucket.spend(rule, state, now)
                              Some(retained.updated(key, Entry(rule, updated))) -> Right(decision)
                          }
                      }
          decision <- F.fromEither(result)
        } yield decision
      }
    }
  }

  trait TokenBucket[F[_]] {
    def decision: F[Decision]
    // Retained for HistoricalLimits and callers that only need admission.
    def take: F[Boolean]
  }

  // Token-bucket logic adapted from http4s Throttle, Copyright 2014 http4s.org, under Apache 2.0.
  // Modified for atomic exact debt accounting and explicit exhaustion delays.
  // Upstream attribution and terms: PROVENANCE.md and third-party-licenses/http4s-LICENSE.
  // https://github.com/http4s/http4s/blob/series/0.23/server/shared/src/main/scala/org/http4s/server/middleware/Throttle.scala#L54
  object TokenBucket {
    private[catsutils] final case class Rule(refillNanos: Long, recoveryNanos: Long)
    private[catsutils] final case class State(debtNanos: Long, at: BigInt)

    private[catsutils] def rule(capacity: Int, refillEvery: Duration): Either[IllegalArgumentException, Rule] = {
      val nanos    = BigInt(refillEvery.getSeconds) * 1000000000L + refillEvery.getNano
      val recovery = nanos * capacity
      if (capacity <= 0) Left(new IllegalArgumentException("capacity must be positive"))
      else if (nanos <= 0) Left(new IllegalArgumentException("refillEvery must be positive"))
      else if (nanos > Long.MaxValue || recovery > Long.MaxValue)
        Left(new IllegalArgumentException("refill and full recovery must fit signed nanoseconds"))
      else Right(Rule(nanos.toLong, recovery.toLong))
    }

    // Keep elapsed arithmetic exact even when timestamps span the signed nanosecond boundary.
    private[catsutils] def now[F[_]: Clock]: F[BigInt] =
      Clock[F].applicative.map(Clock[F].monotonic)(value => BigInt(value.length) * value.unit.toNanos(1L))

    private[catsutils] def remaining(state: State, now: BigInt): Long =
      (BigInt(state.debtNanos) - (now - state.at).max(BigInt(0))).max(BigInt(0)).toLong

    private[catsutils] def spend(rule: Rule, state: State, now: BigInt): (State, Decision) = {
      val debt      = remaining(state, now)
      val at        = now.max(state.at)
      val available = rule.recoveryNanos - rule.refillNanos
      if (debt <= available) State(debt + rule.refillNanos, at) -> Allowed
      else State(debt, at)                                      -> Exhausted(Duration.ofNanos(debt - available))
    }

    def make[F[_]: MonadThrow: Clock: Ref.Make](capacity: Int, refillEvery: Duration): F[TokenBucket[F]] = for {
      policy  <- MonadThrow[F].fromEither(rule(capacity, refillEvery))
      time    <- now[F]
      counter <- Ref.of[F, State](State(0L, time))
    } yield new TokenBucket[F] {
      override val decision: F[Decision] =
        now[F].flatMap(time => counter.modify(state => spend(policy, state, time)))
      override val take: F[Boolean]      = decision.map(_.isAllowed)
    }
  }
}
