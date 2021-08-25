import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}
import dota.etl.WSClient
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class WSClientTest extends AnyFunSuite {

  implicit val system: ActorSystem = ActorSystem()
  system.registerOnTermination {
    System.exit(0)
  }
  implicit val materializer: Materializer = SystemMaterializer(system).materializer

  // WSClient
  val wsClient = new WSClient()

  // TODO: we are missing assertions for these tests but they are good proving grounds to reduce overall number of API calls

  test("recentMatches method") {

    val maybeReply = wsClient.recentMatches()
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

  test("matches method") {

    val maybeReply = wsClient.matches()
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

  test("inspectMatch method") {

    val exampleMatch = 6135452599L
    val maybeReply = wsClient.inspectMatch(exampleMatch)
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

  test("inspectMatches method") {

    // scalastyle:off
    val exampleMatches = Seq(6135452599L, 6135408545L, 6134018090L, 6127211062L, 6126666493L,
      6126623611L, 6126574298L, 6125948851L, 6125877819L, 6125833122L)
    // scalastyle:on

    val maybeReply = wsClient.inspectMatches(exampleMatches)
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

}
