package dota.etl

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DotaETL {

  def main(args: Array[String]): Unit = {

    val input = {
      InputLoop.start()
      InputLoop.getInput
    }

    val maybeReply = input match {
      case 10 => WSClient.recentMatches()
      case _ => WSClient.matches()
    }

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