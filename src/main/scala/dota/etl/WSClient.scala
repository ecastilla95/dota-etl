package dota.etl

import akka.actor.ActorSystem
import akka.stream.{Materializer, SystemMaterializer}
import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

object WSClient {

  // Create Akka system for thread and streaming management
  implicit val system: ActorSystem = ActorSystem()
  system.registerOnTermination { System.exit(0) }

  implicit val materializer: Materializer = SystemMaterializer(system).materializer

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

  def close(): Unit = wsClient.close()

}