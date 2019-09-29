package catuaba

import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.postgres._
import doobie.free.connection.unit
import cats._
import cats.effect._
import cats.implicits._
import fs2.Stream
import org.postgresql.PGProperty
import doobie.util.transactor.Strategy
import org.postgresql.replication.LogSequenceNumber

object Main extends IOApp {
  import Queries._

  def initReplicationSlot = {
    for {
      slotInfo <- getSlotInfo
      _        <- putStrLn(show"~> $slotInfo")
      startPosition <- slotInfo match {
        case None =>
          for {
            createdSlot <- createReplicationSlot
            _           <- putStrLn(show"~> $createdSlot")
          } yield createdSlot.walStartLSN
        case Some(si) =>
          FC.pure(si.latestFlushedLSN)
      }
    } yield startPosition
  }

  def startStreaming = {
    for {
      startPos <- initReplicationSlot
      _        <- putStrLn(s"startPos = $startPos")
    } yield ()
  }

  // create a replication connection and start streaming:
  // 1. create a connection ensuring that
  // 1.a. slot exists
  // 1.b. slot isn't used
  // 2. query to fetch start position in the slot (lsn)
  // 3. start streaming

  val props = new java.util.Properties()
  PGProperty.USER.set(props, "repl_user")
  PGProperty.PASSWORD.set(props, "docker")
  PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "11")
  PGProperty.REPLICATION.set(props, "database")
  PGProperty.PREFER_QUERY_MODE.set(props, "simple")

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:5432/$database",
    props,
    Blocker.liftExecutionContext(ExecutionContexts.synchronous)
  )

  val noCommit = Transactor.strategy.set(xa, Strategy.default.copy(after = unit, before = unit, oops = unit))

  override def run(args: List[String]): IO[ExitCode] = {
    startStreaming.transact(noCommit).as(ExitCode.Success)
  }

  object Queries {
    val slotName = "test_slot"
    val database = "world"
    val plugin   = "pgoutput"

    implicit val lsnMeta: Meta[LogSequenceNumber] = Meta[String]
      .imap(LogSequenceNumber.valueOf(_))(_.asString())

    case class SlotInfo(active: Boolean, confirmedFlushLSN: LogSequenceNumber, restartLSN: LogSequenceNumber, xmin: Option[String]) {
      val latestFlushedLSN = confirmedFlushLSN.asLong()
    }

    object SlotInfo {
      implicit val show: Show[SlotInfo] = Show.fromToString
    }

    def getSlotInfo: ConnectionIO[Option[SlotInfo]] = {
      val select =
        fr"select active, confirmed_flush_lsn, restart_lsn, xmin from pg_replication_slots"
      val condition =
        fr"where slot_name = $slotName and database = $database and plugin = $plugin"

      val statement = select ++ condition
      statement.query[SlotInfo].option
    }

    case class SlotCreated(slotName: String, consistentPoint: Long, snapshotName: String, outputPlugin: String) {
      val walStartLSN = consistentPoint
    }
    object SlotCreated {
      implicit val show: Show[SlotCreated] = Show.fromToString
    }

    def createReplicationSlot = {
      for {
        replicationAPI <- PHC.pgGetConnection(PFPC.getReplicationAPI)
        info <- FC.delay(
          replicationAPI.createReplicationSlot.logical
            .withOutputPlugin(plugin)
            .withSlotName(slotName)
            // .withTemporaryOption()
            .make()
        )
      } yield SlotCreated(info.getSlotName(), info.getConsistentPoint().asLong(), info.getSnapshotName(), info.getOutputPlugin())
    }
  }

  def putStrLn(s: => String): ConnectionIO[Unit] =
    FC.delay(println(s))

}
