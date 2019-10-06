package catuaba

import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.postgres._
import doobie.free.connection.unit
import doobie.util.transactor.Strategy
import cats._
import cats.effect._
import cats.implicits._
import fs2.Stream
import org.postgresql.PGProperty
import org.postgresql.replication.LogSequenceNumber
import java.util.concurrent.TimeUnit

object Main extends IOApp {
  import Queries._

  def initReplicationSlot = {
    for {
      serverIdentity <- identifySystem
      _              <- putStrLn(show"~> $serverIdentity")
      slotInfo       <- getSlotInfo
      _              <- putStrLn(show"~> $slotInfo")
      startPosition <- slotInfo match {
        case None =>
          for {
            createdSlot <- createReplicationSlot
            _           <- putStrLn(show"~> $createdSlot")
          } yield createdSlot.walStartLSN
        case Some(si) =>
          val resolvedPos =
            if (si.confirmedFlushLSN.compareTo(serverIdentity.walFlushLocationLSN) < 0)
              serverIdentity.walFlushLocationLSN
            else
              si.confirmedFlushLSN
          FC.pure(resolvedPos)
      }
    } yield startPosition
  }

  def initPublication = {
    for {
      nrOfPublications <- countOfPublications
      _ <- if (nrOfPublications == 0L) {
        putStrLn("creating new publication") *> createPublication
      } else {
        putStrLn("reusing publication")
      }
    } yield ()
  }

  def startStreaming = {
    for {
      startPos <- initReplicationSlot
      _        <- putStrLn(s"startPos = $startPos")
      _        <- initPublication
      _ <- startReplicationStream(startPos).use { pgStream =>
        for {
          _ <- putStrLn(s"initializing stream ${pgStream.getLastReceiveLSN()}")
        } yield ()
      }
    } yield ()
  }

  // create a replication connection and start streaming:
  // 1. create a connection ensuring that
  // 1.a. slot exists
  // 1.b. slot isn't used
  // 2. query to fetch start position in the slot (lsn)
  // 3. start streaming

  val props = new java.util.Properties()
  PGProperty.USER.set(props, "postgres")
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
    val slotName             = "test_slot"
    val database             = "world"
    val plugin               = "pgoutput"
    val publicationName      = "test_publication"
    val statusUpdateInterval = 10

    implicit val lsnMeta: Meta[LogSequenceNumber] = Meta[String]
      .imap(LogSequenceNumber.valueOf(_))(_.asString())

    case class SlotInfo(active: Boolean, confirmedFlushLSN: LogSequenceNumber, restartLSN: LogSequenceNumber, xmin: Option[String]) {
      val latestFlushedLSN = confirmedFlushLSN
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

    case class SlotCreated(slotName: String, consistentPoint: LogSequenceNumber, snapshotName: String, outputPlugin: String) {
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
      } yield SlotCreated(info.getSlotName(), info.getConsistentPoint(), info.getSnapshotName(), info.getOutputPlugin())
    }

    def startReplicationStream(startPos: LogSequenceNumber) = {
      val stream = for {
        replicationAPI <- PHC.pgGetConnection(PFPC.getReplicationAPI)
        replStream <- FC.delay {
          replicationAPI
            .replicationStream()
            .logical()
            .withSlotName(slotName)
            .withStartPosition(startPos)
            .withSlotOption("proto_version", 1)
            .withSlotOption("publication_names", publicationName)
            .withStatusInterval(statusUpdateInterval, TimeUnit.SECONDS)
            .start()
        }
      } yield replStream

      Resource.make(stream)(s => FC.delay(s.close()))
    }

    case class ServerIdentity(systemId: String, xlogpos: String) {
      lazy val walFlushLocationLSN = LogSequenceNumber.valueOf(xlogpos.toLong)
    }
    object ServerIdentity {
      implicit val show: Show[ServerIdentity] = Show.fromToString
    }

    def identifySystem: ConnectionIO[ServerIdentity] = {
      val statement = sql"IDENTIFY_SYSTEM"
      statement.query[ServerIdentity].unique
    }

    def countOfPublications: ConnectionIO[Long] = {
      sql"select count(1) from pg_publication where pubname = $publicationName".query[Long].unique
    }

    def createPublication: ConnectionIO[Unit] = {
      Update0(s"create publication $publicationName for all tables", None).run.void
    }
  }

  def putStrLn(s: => String): ConnectionIO[Unit] =
    FC.delay(println(s))

}
