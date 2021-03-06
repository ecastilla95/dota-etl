package dota.etl

import akka.actor.ActorSystem
import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

class WSClient(implicit val system: ActorSystem) {

  // Create the standalone WS client
  // no argument defaults to a AhcWSClientConfig created from
  // "AhcWSClientConfigFactory.forConfig(ConfigFactory.load, this.getClass.getClassLoader)"
  val wsClient: StandaloneAhcWSClient = StandaloneAhcWSClient()

  /**
   * Calls API to get recent matches
   * @return recent matches reply
   */
  def recentMatches(): Future[String] = {
    // https://api.opendota.com/api/players/639740/recentMatches
    val account_id = 639740
    val url = s"https://api.opendota.com/api/players/${account_id}/recentMatches"
    wsClient.url(url).get().map(_.body[String])
  }

  /**
   * Calls API to get matches
   * @return matches reply
   */
  def matches(): Future[String] = {
    // https://api.opendota.com/api/players/639740/matches
    val account_id = 639740
    val url = s"https://api.opendota.com/api/players/${account_id}/recentMatches"
    wsClient.url(url).get().map(_.body[String])
  }

  /**
   * Calls API to get match details
   * @return match details
   */
  def inspectMatch(id: Long): Future[String] = {
    // https://api.opendota.com/api/matches/$id
    val url = s"https://api.opendota.com/api/matches/${id}"
    wsClient.url(url).get().map(_.body[String])
  }

  /**
   * Calls API to get match details from a list of matches
   * @return match details list
   */
  def inspectMatches(ids: Seq[Long]): Future[String] =
    Future.sequence(ids.map(inspectMatch))
      .map(list => "[" + list.reduce(_ + "," + _) + "]")

  /**
   * Close the WSClient
   */
  def close(): Unit = wsClient.close()

}

object WSClient {

  val DefaultMatchesSize = 10

  /**
   * Method used for handling futures
   * @param future future to handle
   * @tparam T return type parameter
   * @return Either an exception or an instance of Tx
   */
  def handleResponse[T](future: Future[T]): Either[Throwable, T] =
    future.value match {
      case Some(value) => value match {
        case Success(good) => Right(good)
        case Failure(exception) => Left(exception)
      }
      case None => Left(new Exception("Connection to the API timed out"))
    }

}
