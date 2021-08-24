package dota.etl

import akka.actor.ActorSystem
import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

class WSClient(implicit val system: ActorSystem) {

  // Create the standalone WS client
  // no argument defaults to a AhcWSClientConfig created from
  // "AhcWSClientConfigFactory.forConfig(ConfigFactory.load, this.getClass.getClassLoader)"
  val wsClient: StandaloneAhcWSClient = StandaloneAhcWSClient()

  def recentMatches(): Future[String] = {
    // https://api.opendota.com/api/players/639740/recentMatches
    val account_id = 639740
    val url = s"https://api.opendota.com/api/players/${account_id}/recentMatches"
    wsClient.url(url).get().map(_.body[String])
  }

  def matches(): Future[String] = {
    // https://api.opendota.com/api/players/639740/matches
    val account_id = 639740
    val url = s"https://api.opendota.com/api/players/${account_id}/recentMatches"
    wsClient.url(url).get().map(_.body[String])
  }

  def inspectMatch(id: Long): Future[String] = {
    // https://api.opendota.com/api/matches/$id
    val url = s"https://api.opendota.com/api/matches/${id}"
    wsClient.url(url).get().map(_.body[String])
  }

  def close(): Unit = wsClient.close()

}

object WSClient {

  val DefaultMatchesSize = 10

}