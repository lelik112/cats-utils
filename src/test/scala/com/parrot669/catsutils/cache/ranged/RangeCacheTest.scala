// Source scenarios are from the owner-supplied Bobylev utility tests; see PROVENANCE.md.
package com.parrot669.catsutils.cache.ranged

import java.time.{Duration, Instant}
import scala.concurrent.duration._

import cats.effect.syntax.all._
import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.{Sync, Temporal}
import cats.syntax.all._

import com.parrot669.catsutils.cache.ranged.RangeCache.Config
import com.parrot669.catsutils.domain.TimeRange

import munit.CatsEffectSuite

class RangeCacheTest extends CatsEffectSuite {
  private val minute  = Duration.ofMinutes(1)
  private val hour    = Duration.ofHours(1)
  private val default = Config.expireAfterWrite(minute)

  private def resource(config: Config = default, reversed: Boolean = false): Resource[IO, RangeCache[IO, String, Int]] =
    RangeCache.make[IO, String, Int]("test", config)(
      Sync[IO],
      Temporal[IO],
      if (reversed) Ordering[Instant].reverse else Ordering[Instant]
    )

  private def run(config: Config = default, reversed: Boolean = false)(
    body: RangeCache[IO, String, Int] => IO[Unit]
  ): IO[Unit] = TestControl.executeEmbed(resource(config, reversed).use(body))

  // Each iteration registers all fourteen original scenarios with the original direction-specific assertions.
  List(false, true).foreach { reversed =>
    val suffix = if (reversed) " (reversed)" else ""
    def ordered[A](values: List[A]): List[A] = if (reversed) values.reverse else values

    test(s"[source] stores and returns values$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _     <- cache.put(
                     "test",
                     range("10:00", "15:00"),
                     Seq(instant("10:30") -> 1, instant("11:00") -> 2, instant("13:00") -> 3)
                   )
          empty <- cache.getOrUpdate("test", range("09:30", "10:00"))(_ => IO.pure(Nil))
          read0 <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ => IO.pure(Nil))
          read1 <- cache.getOrUpdate("test", range("10:00", "15:00"))(_ => IO.pure(Nil))
        } yield {
          assert(empty.isEmpty)
          assertEquals(read0, List(1))
          assertEquals(read1, ordered(List(1, 2, 3)))
        }
      }
    }

    test(s"[source] replaces values$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _    <- cache.put(
                    "test",
                    range("10:00", "15:00"),
                    Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("13:00") -> 3)
                  )
          _    <- cache.put("test", range("12:00", "13:30"), Seq(instant("12:00") -> 4))
          read <- cache.getOrUpdate("test", range("10:00", "15:00"))(_ => IO.pure(Nil))
        } yield assertEquals(read, ordered(List(1, 2, 4)))
      }
    }

    test(s"[source] waits for the load to finish before putting$suffix") {
      run(reversed = reversed) { cache =>
        for {
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          loading <- cache
                       .getOrUpdate("test", range("10:00", "13:00")) { _ =>
                         started.complete(()) *> release.get.as(
                           Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3)
                         )
                       }
                       .start
          _       <- started.get
          putting <- cache.put("test", range("12:00", "13:00"), Seq(instant("12:00") -> 4, instant("12:30") -> 5)).start
          _       <- IO.cede *> release.complete(())
          _       <- loading.joinWithNever *> putting.joinWithNever
          read    <- cache.getOrUpdate("test", range("10:00", "13:00"))(_ => IO.pure(Seq(instant("12:00") -> 4)))
        } yield assertEquals(read, ordered(List(1, 2, 4, 5)))
      }
    }

    test(s"[source] fills gaps in the data$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _      <- cache.put(
                      "test",
                      range("10:00", "13:00"),
                      Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3)
                    )
          gaps   <- Ref.of[IO, List[TimeRange]](Nil)
          read   <-
            cache.getOrUpdate("test", range("05:00", "15:00"))(gap => gaps.update(_ :+ gap).as(Seq(gap.start -> 4)))
          loaded <- gaps.get
        } yield {
          assertEquals(loaded, ordered(List(range("05:00", "10:00"), range("13:00", "15:00"))))
          assertEquals(read, ordered(List(4, 1, 2, 3, 4)))
        }
      }
    }

    test(s"[source] splits big gaps into pieces$suffix") {
      run(default.withMaxGap(hour), reversed) { cache =>
        for {
          gaps   <- Ref.of[IO, List[TimeRange]](Nil)
          _      <- cache.getOrUpdate("test", range("10:00", "13:00"))(gap => gaps.update(_ :+ gap).as(Nil))
          loaded <- gaps.get
        } yield assertEquals(
          loaded,
          ordered(List(range("10:00", "11:00"), range("11:00", "12:00"), range("12:00", "13:00")))
        )
      }
    }

    test(s"[source] combines gaps if there is a small data interval between them$suffix") {
      run(default.withIgnoreRanges(Duration.ofMinutes(30)), reversed) { cache =>
        val expected = Seq(instant("12:00") -> 3, instant("12:30") -> 4)
        for {
          gaps   <- Ref.of[IO, List[TimeRange]](Nil)
          _      <- cache.put("test", range("11:00", "11:30"), Seq(instant("11:15") -> 1))
          _      <- cache.put("test", range("13:00", "14:00"), Seq(instant("11:15") -> 2))
          read   <- cache.getOrUpdate("test", range("10:00", "15:00"))(gap => gaps.update(_ :+ gap).as(expected))
          loaded <- gaps.get
        } yield {
          assertEquals(loaded, ordered(List(range("10:00", "13:00"), range("14:00", "15:00"))))
          assertEquals(read, ordered(expected.map(_._2).toList))
        }
      }
    }

    test(s"[source] splits big gaps after combining them$suffix") {
      run(default.withMaxGap(hour).withIgnoreRanges(Duration.ofMinutes(30)), reversed) { cache =>
        for {
          gaps   <- Ref.of[IO, List[TimeRange]](Nil)
          _      <- cache.put("test", range("11:00", "11:30"), Nil)
          _      <- cache.getOrUpdate("test", range("10:00", "13:00"))(gap => gaps.update(_ :+ gap).as(Nil))
          loaded <- gaps.get
        } yield assertEquals(
          loaded,
          ordered(List(range("10:00", "11:00"), range("11:00", "12:00"), range("12:00", "13:00")))
        )
      }
    }

    test(s"[source] waits for the data to load$suffix") {
      run(reversed = reversed) { cache =>
        for {
          count <- Ref.of[IO, Int](0)
          load   = IO.sleep(1.second) *> count
                     .update(_ + 1)
                     .as(Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3))
          both  <- (
                     cache.getOrUpdate("test", range("05:00", "14:00"))(_ => load),
                     cache.getOrUpdate("test", range("05:00", "14:00"))(_ => load)
                   ).parTupled
          calls <- count.get
        } yield {
          assertEquals(calls, 1)
          assertEquals(both._1, ordered(List(1, 2, 3)))
          assertEquals(both._2, ordered(List(1, 2, 3)))
        }
      }
    }

    test(s"[source] does not wait if data is readily available$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _       <- cache.put(
                       "test",
                       range("10:00", "13:00"),
                       Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3)
                     )
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          loading <- cache
                       .getOrUpdate("test", range("05:00", "15:00"))(_ => started.complete(()) *> release.get.as(Nil))
                       .start
          _       <- started.get
          read    <- cache.getOrUpdate("test", range("10:00", "13:00"))(_ => IO.pure(Nil))
          _       <- release.complete(()) *> loading.joinWithNever
        } yield assertEquals(read, ordered(List(1, 2, 3)))
      }
    }

    test(s"[source] invalidates data$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _    <- cache.put(
                    "test",
                    range("10:00", "13:00"),
                    Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3)
                  )
          _    <- cache.invalidate("test", instant("12:00"))
          read <- cache.getOrUpdate("test", range("10:00", "13:00"))(_ => IO.pure(Seq(instant("12:00") -> 4)))
        } yield assertEquals(read, ordered(List(1, 2, 4)))
      }
    }

    test(s"[source] invalidates ongoing load$suffix") {
      run(reversed = reversed) { cache =>
        for {
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          loading <- cache
                       .getOrUpdate("test", range("10:00", "13:00")) { _ =>
                         started.complete(()) *> release.get.as(
                           Seq(instant("10:00") -> 1, instant("11:00") -> 2, instant("12:00") -> 3)
                         )
                       }
                       .start
          _       <- started.get *> cache.invalidate("test", instant("12:00")) *> release.complete(())
          _       <- loading.joinWithNever
          read    <- cache.getOrUpdate("test", range("10:00", "13:00"))(_ => IO.pure(Seq(instant("12:00") -> 4)))
        } yield assertEquals(read, ordered(List(1, 2, 4)))
      }
    }

    test(s"[source] does not cache None parts$suffix") {
      run(default.withMaxGap(hour), reversed) { cache =>
        cache
          .getOrUpdateOption("test", range("10:00", "15:00")) { range =>
            IO.pure(Option.when(range.from.getHour != 13)(Seq(range.start -> 1)))
          }
          .map(read => assertEquals(read, List(1, 1, 1, 1)))
      }
    }

    test(s"[source] skips filling last interval$suffix") {
      TestControl.executeEmbed(
        IO.sleep(instant("12:00").toEpochMilli.millis) *>
          resource(default.withSkipLast(hour), reversed).use { cache =>
            for {
              _     <- cache.put("test", range("02:00", "11:00"), Seq(instant("02:15") -> 1))
              read0 <- cache.getOrUpdate("test", range("00:00", "12:00")) {
                         case gap if gap.start == instant("00:00") => IO.pure(Seq(instant("00:15") -> 0))
                         case _                                    => IO.raiseError(new AssertionError("boom!"))
                       }
              _     <- cache.put("test", range("11:15", "11:30"), Seq(instant("11:15") -> 2))
              read1 <-
                cache.getOrUpdate("test", range("05:00", "12:00"))(_ => IO.raiseError(new AssertionError("boom!")))
            } yield {
              assertEquals(read0, ordered(List(0, 1)))
              assertEquals(read1, List(2))
            }
          }
      )
    }

    test(s"[source] does not add entries on invalidate$suffix") {
      run(reversed = reversed) { cache =>
        for {
          _    <- cache.invalidate("foo", instant("10:00"))
          _    <- cache.invalidate("bar", instant("11:00"))
          size <- cache.size
        } yield assertEquals(size, 0L)
      }
    }
  }

  test("[source] trims ranges to now") {
    run(default.withTrimToNow) { cache =>
      for {
        now      <- IO.realTimeInstant
        before    = now.minusSeconds(60)
        after     = now.plusSeconds(60)
        future   <- TimeRange.makeF[IO](now, after)
        read0    <- cache.getOrUpdate("test", future)(_ => IO.raiseError(new AssertionError("boom!")))
        overlaps <- TimeRange.makeF[IO](before, after.plusSeconds(60))
        read1    <- cache.getOrUpdate("test", overlaps)(_ => IO.pure(Seq(before -> 1, after -> 2)))
      } yield {
        assert(read0.isEmpty)
        assertEquals(read1, List(1))
      }
    }
  }

  test("[cats-effect] sequential business failure ends the stream after preserving earlier commits") {
    run(default.withMaxGap(hour)) { cache =>
      for {
        calls     <- Ref.of[IO, List[TimeRange]](Nil)
        result    <- cache
                       .streamEither[String]("test", range("10:00", "13:00")) { gap =>
                         calls
                           .update(_ :+ gap)
                           .as(
                             if (gap.start == instant("11:00")) Left("unavailable") else Right(Some(Seq(gap.start -> 1)))
                           )
                       }
                       .compile
                       .toList
        loaded    <- calls.get
        cached    <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ =>
                       IO.raiseError(new AssertionError("already committed"))
                     )
        recovered <- cache.getOrUpdate("test", range("11:00", "13:00"))(gap => IO.pure(Seq(gap.start -> 2)))
      } yield {
        assertEquals(result, List(Right(1), Left("unavailable")))
        assertEquals(loaded, List(range("10:00", "11:00"), range("11:00", "12:00")))
        assertEquals(cached, List(1))
        assertEquals(recovered, List(2, 2))
      }
    }
  }

  test("[cats-effect] a later parallel business failure cancels an earlier never-ending gap") {
    run(default.withMaxGap(hour).withParallelism(2)) { cache =>
      for {
        started   <- Deferred[IO, Unit]
        canceled  <- Ref.of[IO, Boolean](false)
        result    <- cache
                       .streamEither[String]("test", range("10:00", "12:00")) { gap =>
                         if (gap.start == instant("10:00"))
                           (started.complete(()) *> IO.never[Either[String, Option[Iterable[(Instant, Int)]]]])
                             .onCancel(IO.cede *> canceled.set(true))
                         else started.get.as(Left("failed second gap"))
                       }
                       .compile
                       .toList
                       .timeout(5.seconds)
        didCancel <- canceled.get
        recovered <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 3)))
      } yield {
        assertEquals(result, List(Left("failed second gap")))
        assert(didCancel)
        assertEquals(recovered, List(3))
      }
    }.replicateA_(20)
  }

  test("[cats-effect] raised exceptions remain effect failures and preserve earlier commits") {
    run(default.withMaxGap(hour)) { cache =>
      val boom = new IllegalStateException("broken loader")
      for {
        result <- cache
                    .getOrUpdateEither[String]("test", range("10:00", "12:00")) { gap =>
                      if (gap.start == instant("10:00")) IO.pure(Right(Seq(gap.start -> 1))) else IO.raiseError(boom)
                    }
                    .attempt
        read   <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ =>
                    IO.raiseError(new AssertionError("already committed"))
                  )
        _      <- cache.put("test", range("11:00", "12:00"), Seq(instant("11:00") -> 2))
      } yield {
        assertEquals(result, Left(boom))
        assertEquals(read, List(1))
      }
    }
  }

  test("[cats-effect] a later parallel raised failure cancels an earlier never-ending gap") {
    run(default.withMaxGap(hour).withParallelism(2)) { cache =>
      val boom = new IllegalStateException("failed second gap")
      for {
        started   <- Deferred[IO, Unit]
        canceled  <- Ref.of[IO, Boolean](false)
        result    <- cache
                       .getOrUpdateEither[String]("test", range("10:00", "12:00")) { gap =>
                         if (gap.start == instant("10:00"))
                           (started.complete(()) *> IO.never[Either[String, Iterable[(Instant, Int)]]])
                             .onCancel(IO.cede *> canceled.set(true))
                         else started.get *> IO.raiseError(boom)
                       }
                       .timeout(5.seconds)
                       .attempt
        didCancel <- canceled.get
        recovered <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 3)))
      } yield {
        assertEquals(result, Left(boom))
        assert(didCancel)
        assertEquals(recovered, List(3))
      }
    }.replicateA_(20)
  }

  test("[cats-effect] taking one stream element keeps its committed part and releases the lock") {
    run(default.withMaxGap(hour)) { cache =>
      for {
        calls <- Ref.of[IO, Int](0)
        first <- cache
                   .stream("test", range("10:00", "13:00"))(gap => calls.update(_ + 1).as(Some(Seq(gap.start -> 1))))
                   .take(1)
                   .compile
                   .toList
        count <- calls.get
        _     <- cache.put("test", range("11:00", "12:00"), Seq(instant("11:00") -> 2))
        read  <- cache.getOrUpdate("test", range("10:00", "12:00"))(_ =>
                   IO.raiseError(new AssertionError("already committed"))
                 )
      } yield {
        assertEquals(first, List(1))
        assertEquals(count, 1)
        assertEquals(read, List(1, 2))
      }
    }
  }

  test("[cats-effect] failure waits for loader finalizers before releasing the key lock or returning") {
    run(default.withMaxGap(hour).withParallelism(2)) { cache =>
      for {
        started               <- Deferred[IO, Unit]
        finalizerStarted      <- Deferred[IO, Unit]
        allowFinish           <- Deferred[IO, Unit]
        finalized             <- Ref.of[IO, Boolean](false)
        terminal              <- Ref.of[IO, Boolean](false)
        waiterStarted         <- Deferred[IO, Unit]
        owner                 <- cache
                                   .streamEither[String]("test", range("10:00", "12:00")) { gap =>
                                     if (gap.start == instant("10:00"))
                                       (started.complete(()) *> IO.never[Either[String, Option[Iterable[(Instant, Int)]]]])
                                         .onCancel(finalizerStarted.complete(()) *> allowFinish.get *> finalized.set(true))
                                     else started.get.as(Left("failed second gap"))
                                   }
                                   .compile
                                   .toList
                                   .flatTap(_ => terminal.set(true))
                                   .start
        _                     <- finalizerStarted.get.timeout(5.seconds)
        waiter                <- cache
                                   .getOrUpdate("test", range("10:00", "11:00")) { gap =>
                                     waiterStarted.complete(()).as(Seq(gap.start -> 3))
                                   }
                                   .start
        // Let all runnable fibers reach their blocking points before inspecting the boundary.
        _                     <- IO.sleep(1.nano)
        returnedBeforeCleanup <- terminal.get
        loadedBeforeCleanup   <- waiterStarted.tryGet
        _                     <- allowFinish.complete(())
        result                <- owner.joinWithNever.timeout(5.seconds)
        recovered             <- waiter.joinWithNever.timeout(5.seconds)
        didFinalize           <- finalized.get
      } yield {
        assert(!returnedBeforeCleanup)
        assertEquals(loadedBeforeCleanup, None)
        assertEquals(result, List(Left("failed second gap")))
        assert(didFinalize)
        assertEquals(recovered, List(3))
      }
    }.replicateA_(20)
  }

  List(1, 2).foreach { parallelism =>
    test(s"[cats-effect] self-canceling loader cancels the request and releases its lock at parallelism $parallelism") {
      run(default.withMaxGap(hour).withParallelism(parallelism)) { cache =>
        for {
          finalized   <- Ref.of[IO, Boolean](false)
          owner       <- cache
                           .getOrUpdate("test", range("10:00", "12:00")) { gap =>
                             if (gap.start == instant("10:00"))
                               (IO.canceled *> IO.never[Iterable[(Instant, Int)]]).onCancel(IO.cede *> finalized.set(true))
                             else IO.never[Iterable[(Instant, Int)]]
                           }
                           .start
          outcome     <- owner.join.timeout(5.seconds)
          didFinalize <- finalized.get
          recovered   <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 3)))
        } yield {
          assert(outcome.isCanceled)
          assert(didFinalize)
          assertEquals(recovered, List(3))
        }
      }.replicateA_(20)
    }
  }

  test("[cats-effect] parallel early termination succeeds and awaits remaining loader finalizers") {
    run(default.withMaxGap(hour).withParallelism(2)) { cache =>
      for {
        secondStarted <- Deferred[IO, Unit]
        finalized     <- Ref.of[IO, Boolean](false)
        first         <- cache
                           .stream("test", range("10:00", "12:00")) { gap =>
                             if (gap.start == instant("10:00")) secondStarted.get.as(Some(Seq(gap.start -> 1)))
                             else
                               (secondStarted.complete(()) *> IO.never[Option[Iterable[(Instant, Int)]]])
                                 .onCancel(IO.cede *> finalized.set(true))
                           }
                           .take(1)
                           .compile
                           .toList
                           .timeout(5.seconds)
        didFinalize   <- finalized.get
        _             <- cache.put("test", range("11:00", "12:00"), Seq(instant("11:00") -> 2))
        read          <-
          cache.getOrUpdate("test", range("10:00", "12:00"))(_ => IO.raiseError(new AssertionError("coverage lost")))
      } yield {
        assertEquals(first, List(1))
        assert(didFinalize)
        assertEquals(read, List(1, 2))
      }
    }.replicateA_(20)
  }

  test("[cats-effect] canceling a loader releases its range permit") {
    run() { cache =>
      for {
        started <- Deferred[IO, Unit]
        owner   <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ => started.complete(()) *> IO.never).start
        _       <- started.get *> owner.cancel
        read    <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 1)))
      } yield assertEquals(read, List(1))
    }
  }

  test("[cats-effect] canceling a waiter does not cancel the range loader") {
    run() { cache =>
      for {
        started   <- Deferred[IO, Unit]
        release   <- Deferred[IO, Unit]
        canceled  <- Ref.of[IO, Boolean](false)
        owner     <- cache
                       .getOrUpdate("test", range("10:00", "11:00")) { gap =>
                         (started.complete(()) *> release.get.as(Seq(gap.start -> 1))).onCancel(canceled.set(true))
                       }
                       .start
        _         <- started.get
        waiter    <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 2))).start
        _         <- IO.cede *> waiter.cancel *> release.complete(())
        result    <- owner.joinWithNever
        didCancel <- canceled.get
        read      <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ =>
                       IO.raiseError(new AssertionError("already committed"))
                     )
      } yield {
        assert(!didCancel)
        assertEquals(result, List(1))
        assertEquals(read, List(1))
      }
    }
  }

  List(false, true).foreach { reversed =>
    test(
      s"[cats-effect] parallel gap loads preserve ${if (reversed) "descending" else "ascending"} output and bound concurrency"
    ) {
      run(default.withMaxGap(hour).withParallelism(2), reversed) { cache =>
        for {
          active      <- Ref.of[IO, Int](0)
          maximum     <- Ref.of[IO, Int](0)
          result      <- cache.getOrUpdate("test", range("10:00", "14:00")) { gap =>
                           Resource
                             .make(active.updateAndGet(_ + 1).flatTap(n => maximum.update(_ max n)))(_ =>
                               active.update(_ - 1)
                             )
                             .use { _ =>
                               IO.sleep((15 - gap.from.getHour).seconds).as(Seq(gap.start -> gap.from.getHour))
                             }
                         }
          peak        <- maximum.get
          finalActive <- active.get
        } yield {
          assertEquals(result, if (reversed) List(13, 12, 11, 10) else List(10, 11, 12, 13))
          assertEquals(peak, 2)
          assertEquals(finalActive, 0)
        }
      }
    }
  }

  test("[cats-effect] invalidation applies to every later partial commit in the same load") {
    run(default.withMaxGap(hour).withParallelism(2)) { cache =>
      for {
        started  <- Deferred[IO, Unit]
        count    <- Ref.of[IO, Int](0)
        release  <- Deferred[IO, Unit]
        old      <- cache
                      .getOrUpdate("test", range("10:00", "14:00")) { gap =>
                        count.updateAndGet(_ + 1).flatMap(n => if (n == 2) started.complete(()).void else IO.unit) *>
                          release.get.as(Seq(gap.start -> gap.from.getHour))
                      }
                      .start
        _        <- started.get *> cache.invalidate("test", instant("11:00")) *> release.complete(())
        original <- old.joinWithNever
        missing  <- Ref.of[IO, List[TimeRange]](Nil)
        fresh    <-
          cache.getOrUpdate("test", range("10:00", "14:00"))(gap => missing.update(_ :+ gap).as(Seq(gap.start -> 99)))
        reloaded <- missing.get
      } yield {
        assertEquals(original, List(10, 11, 12, 13))
        assertEquals(fresh, List(10, 99, 99, 99))
        assertEquals(reloaded.size, 3)
        assertEquals(
          reloaded.sorted(TimeRange.ordering),
          List(range("11:00", "12:00"), range("12:00", "13:00"), range("13:00", "14:00"))
        )
      }
    }
  }

  test("[cats-effect] expired loads cannot overwrite a replacement generation") {
    run() { cache =>
      for {
        started     <- Deferred[IO, Unit]
        release     <- Deferred[IO, Unit]
        old         <- cache
                         .getOrUpdate("test", range("10:00", "11:00"))(gap =>
                           started.complete(()) *> release.get.as(Seq(gap.start -> 1))
                         )
                         .start
        _           <- started.get *> IO.sleep(61.seconds)
        replacement <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 2)))
        _           <- release.complete(()) *> old.joinWithNever
        read        <-
          cache.getOrUpdate("test", range("10:00", "11:00"))(_ => IO.raiseError(new AssertionError("replacement lost")))
      } yield {
        assertEquals(replacement, List(2))
        assertEquals(read, List(2))
      }
    }
  }

  test("[cats-effect] None leaves a gap loadable while Some(empty) records coverage") {
    run(default.withMaxGap(hour)) { cache =>
      for {
        _       <- cache.getOrUpdateOption("test", range("10:00", "12:00")) { gap =>
                     IO.pure(if (gap.start == instant("10:00")) None else Some(Nil))
                   }
        missing <- Ref.of[IO, List[TimeRange]](Nil)
        read    <-
          cache.getOrUpdate("test", range("10:00", "12:00"))(gap => missing.update(_ :+ gap).as(Seq(gap.start -> 1)))
        loaded  <- missing.get
      } yield {
        assertEquals(read, List(1))
        assertEquals(loaded, List(range("10:00", "11:00")))
      }
    }
  }

  test("[cats-effect] full invalidation removes data, keys, and resident size") {
    run(default.withMaxWeight(10)) { cache =>
      for {
        _      <- cache.put("test", range("10:00", "11:00"), Seq(instant("10:00") -> 1))
        before <- (cache.keys, cache.size, cache.weight).tupled
        _      <- cache.invalidate("test")
        after  <- (cache.keys, cache.size, cache.weight).tupled
      } yield {
        assertEquals(before, (Set("test"), 1L, 1L))
        assertEquals(after, (Set.empty[String], 0L, 0L))
      }
    }
  }

  List(Config.expireAfterWrite(Duration.ZERO), Config.expireAfterAccess(Duration.ZERO)).zipWithIndex.foreach {
    case (config, index) =>
      test(s"[cats-effect] zero expiration mode $index completes requests without retaining data") {
        run(config) { cache =>
          for {
            first  <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 1)))
            second <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 2)))
            size   <- cache.size
          } yield {
            assertEquals(first, List(1))
            assertEquals(second, List(2))
            assertEquals(size, 0L)
          }
        }
      }
  }

  test("[cats-effect] zero maximum weight makes progress and evicts positive-weight results") {
    run(default.withMaxWeight(0)) { cache =>
      for {
        first  <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 1)))
        weight <- cache.weight
        second <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 2)))
        _      <- cache.put("empty", range("10:00", "11:00"), Nil)
        empty  <- cache.getOrUpdate("empty", range("10:00", "11:00"))(_ =>
                    IO.raiseError(new AssertionError("zero-weight coverage lost"))
                  )
      } yield {
        assertEquals(first, List(1))
        assertEquals(second, List(2))
        assertEquals(weight, 0L)
        assert(empty.isEmpty)
      }
    }
  }

  test("[cats-effect] oversized loaded ranges are returned but are not retained") {
    run(default.withMaxWeight(1)) { cache =>
      val values = Seq(instant("10:00") -> 1, instant("10:30") -> 2)
      for {
        calls  <- Ref.of[IO, Int](0)
        first  <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ => calls.update(_ + 1).as(values))
        second <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ => calls.update(_ + 1).as(values))
        count  <- calls.get
        weight <- cache.weight
      } yield {
        assertEquals(first, List(1, 2))
        assertEquals(second, List(1, 2))
        assertEquals(count, 2)
        assertEquals(weight, 0L)
      }
    }
  }

  test("[cats-effect] access expiration is extended by cache hits") {
    run(Config.expireAfterAccess(Duration.ofSeconds(10))) { cache =>
      for {
        _      <- cache.put("test", range("10:00", "11:00"), Seq(instant("10:00") -> 1))
        _      <- IO.sleep(9.seconds)
        first  <-
          cache.getOrUpdate("test", range("10:00", "11:00"))(_ => IO.raiseError(new AssertionError("premature expiry")))
        _      <- IO.sleep(9.seconds)
        second <- cache.getOrUpdate("test", range("10:00", "11:00"))(_ =>
                    IO.raiseError(new AssertionError("access not refreshed"))
                  )
        _      <- IO.sleep(11.seconds)
        third  <- cache.getOrUpdate("test", range("10:00", "11:00"))(gap => IO.pure(Seq(gap.start -> 3)))
      } yield {
        assertEquals(first, List(1))
        assertEquals(second, List(1))
        assertEquals(third, List(3))
      }
    }
  }

  test("[cats-effect] invalid gap and parallelism configuration is rejected") {
    intercept[IllegalArgumentException](default.withMaxGap(Duration.ZERO))
    intercept[IllegalArgumentException](default.withMaxGap(Duration.ofNanos(-1)))
    intercept[IllegalArgumentException](default.withParallelism(0))
    intercept[IllegalArgumentException](default.withIgnoreRanges(Duration.ofNanos(-1)))
    intercept[IllegalArgumentException](default.withSkipLast(Duration.ofNanos(-1)))
    intercept[IllegalArgumentException](default.withMaxWeight(-1))
  }

  private def instant(time: String): Instant = Instant.parse(s"2025-02-19T$time:00Z")

  private def range(from: String, until: String): TimeRange =
    TimeRange.make(instant(from), instant(until)).getOrElse(sys.error("boom"))
}
