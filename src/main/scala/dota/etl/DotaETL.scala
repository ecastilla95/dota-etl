package dota.etl

import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}
import dota.etl.WSClient.DefaultMatchesSize
import dota.etl.spark.{DataAnalytics, JsonParser}
import org.apache.spark.sql.DataFrame

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Success

object DotaETL {

  def main(args: Array[String]): Unit = {

    // Start the process and the timer
    Timer.time {

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

      // Not really necessary but it doesn't hurt to call a more specific method when possible
      val maybeReply = input match {
        case DefaultMatchesSize => wsClient.recentMatches()
        case _ => wsClient.matches()
      }

      Await.ready(maybeReply, Duration.Inf)
      // This is kinda bad but I am not used to working with Futures.
      val value = maybeReply.value.orNull.get

      // We inspect the response in order to find the last played matches
      val playerMatchHistory = JsonParser.parsePlayerMatchHistory(value, input)
      val matchIds = DataAnalytics.extractIds(playerMatchHistory)

      // We call the API for details on the last played matches
      val maybeMatches: Future[String] = wsClient.inspectMatches(matchIds)

      Await.ready(maybeMatches, Duration.Inf)
      val matches = maybeReply.value.orNull.get
      val parsedMatches = JsonParser.parseMatchList(matches)

      // We start handling the dataframe operations
      val playerPerformance = DataAnalytics.playerPerformance(playerMatchHistory)
      val teamKills = DataAnalytics.teamKills(parsedMatches)
      val completePerformance = DataAnalytics.completePerformance(teamKills, playerPerformance)

      completePerformance.show()

      JsonParser.close()
      system.terminate()
    }
  }
}
