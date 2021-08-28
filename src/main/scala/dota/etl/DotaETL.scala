package dota.etl

import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}
import dota.etl.WSClient.DefaultMatchesSize
import dota.etl.spark.{DataAnalytics, JsonParser}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object DotaETL {

  def main(args: Array[String]): Unit = {

    val timeout = 60 seconds

    // Create Akka system for thread and streaming management
    implicit val system: ActorSystem = ActorSystem()
    system.registerOnTermination {
      System.exit(0)
    }
    implicit val materializer: Materializer = SystemMaterializer(system).materializer

    // WSClient
    val wsClient = new WSClient()

    val input = {
      InputLoop.start()
      InputLoop.getInput
    }

    // Start the process and the timer
    Timer.time {

      // Not really necessary but it doesn't hurt to call a more specific method when possible
      val maybeReply = input match {
        case DefaultMatchesSize => wsClient.recentMatches()
        case _ => wsClient.matches()
      }

      Await.ready(maybeReply, timeout)
      // This is kinda bad but I am not used to working with Futures.
      val eitherValueOrException = WSClient.handleResponse(maybeReply)

      // We inspect the response in order to find the last played matches
      val eitherResultOrException = eitherValueOrException.map { value =>
        val playerMatchHistory = JsonParser.parsePlayerMatchHistory(value, input)
        val matchIds = DataAnalytics.extractIds(playerMatchHistory)
        // We call the API for details on the last played matches
        val maybeMatches: Future[String] = wsClient.inspectMatches(matchIds)

        Await.ready(maybeMatches, timeout)
        val eitherMatchesOrException = WSClient.handleResponse(maybeMatches)
        eitherMatchesOrException.map { matches =>
          val parsedMatches = JsonParser.parseMatchList(matches)
          // See DataAnalytics to understand the dataframe operations
          DataAnalytics.analyze((playerMatchHistory, parsedMatches))
        }
      }

      println(eitherResultOrException)

      JsonParser.close()
      system.terminate()
    }
  }
}
