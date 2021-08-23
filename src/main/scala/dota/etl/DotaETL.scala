package dota.etl

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object DotaETL {

  def main(args: Array[String]): Unit = {

    val input = InputLoop.getInput

    val maybeReply = WSClient.recentMatches()

    Await.ready(maybeReply, Duration.Inf)

    // This is kinda bad but I am not used to getting data from Futures and I don't know create DFs inside one.
    /**
    maybeReply.onComplete {
      case Success(value) => new DotaSpark(value)
      case Failure(exception) => sys.error(exception.getMessage)
    }
    */
    val value = maybeReply.value.orNull.get

    // We move onto Spark
    new DotaSpark(value)

    WSClient.close()
  }
}