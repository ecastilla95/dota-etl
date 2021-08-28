package dota.etl

import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}
import com.typesafe.scalalogging.Logger
import dota.etl.WSClient.DefaultMatchesSize
import dota.etl.spark.{DataAnalytics, JsonParser}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object DotaETL {

  /**
   * Main method
   * @param args not used
   */
  def main(args: Array[String]): Unit = {

    // Fancy way of getting the logger to have the object name
    val objectName = this.getClass.getName.filter(_ != '$')
    implicit val logger: Logger = Logger(objectName)

    logger.info(s"$objectName's main method starting")

    val timeout = 60 seconds

    // Create Akka system for thread and streaming management
    implicit val system: ActorSystem = ActorSystem()
    system.registerOnTermination {
      System.exit(0)
    }
    implicit val materializer: Materializer = SystemMaterializer(system).materializer

    // WSClient
    logger.info(s"Initializing web socket client")
    val wsClient = new WSClient()

    val input = {
      InputLoop.start()
      InputLoop.getInput
    }

    logger.info(s"Input is $input, starting the timer...")
    // Start the process and the timer
    Timer.time {

      // Not really necessary but it doesn't hurt to call a more specific method when possible
      val maybeReply = input match {
        case DefaultMatchesSize => wsClient.recentMatches()
        case _ => wsClient.matches()
      }

      logger.info(s"Awaiting $timeout for API response")
      Await.ready(maybeReply, timeout)
      val eitherValueOrException = WSClient.handleResponse(maybeReply)

      // We inspect the response in order to find the last played matches
      val eitherResultOrException = eitherValueOrException.flatMap { value =>
        val playerMatchHistory = JsonParser.parsePlayerMatchHistory(value, input)
        val matchIds = DataAnalytics.extractIds(playerMatchHistory)
        // We call the API for details on the last played matches
        val maybeMatches: Future[String] = wsClient.inspectMatches(matchIds)

        logger.info(s"Awaiting $timeout for API response")
        Await.ready(maybeMatches, timeout)
        val eitherMatchesOrException = WSClient.handleResponse(maybeMatches)
        eitherMatchesOrException.map { matches =>
          val parsedMatches = JsonParser.parseMatchList(matches)
          // See DataAnalytics to understand the dataframe operations
          DataAnalytics.analyze((playerMatchHistory, parsedMatches))
        }
      }

      eitherResultOrException match {
        case Right(value) => logger.info(
          s"""
            |Logging data for Dota games of user YrikGood:
            |${JsonParser.prettify(value)}
            |""".stripMargin)
        case Left(exception) => logger.info(JsonParser.prettify(s"""{"exception": "${exception.getMessage}"}"""))
      }

      logger.info(s"Matches studied: $input")
      JsonParser.close()
      system.terminate()
    }
  }
}
