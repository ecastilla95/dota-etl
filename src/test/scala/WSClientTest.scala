import dota.etl.WSClient
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class WSClientTest extends AnyFunSuite {

  test("Recent Matches") {

    val maybeReply = WSClient.recentMatches()
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete{
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

  test("Matches") {

    val maybeReply = WSClient.matches()
    Await.ready(maybeReply, Duration.Inf)

    maybeReply.onComplete{
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }

  }

}
