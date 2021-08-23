package dota.etl

import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DotaETL {

  def main(args: Array[String]): Unit = {

    // Create Akka system for thread and streaming management
    implicit val system: ActorSystem = ActorSystem()
    system.registerOnTermination { System.exit(0) }
    implicit val materializer: Materializer = SystemMaterializer(system).materializer

    // WSClient
    val wsClient = new WSClient()

    val input = {
      InputLoop.start()
      InputLoop.getInput
    }

    val maybeReply = input match {
      case 10 => wsClient.recentMatches()
      case _ => wsClient.matches()
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
    val ds = new DotaSpark(value)

    ds.close()
    system.terminate()
  }
}