import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.model.StatusCode.int2StatusCode
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.model.HttpMethods._

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}
import spray.json.DefaultJsonProtocol._

case class Match(
                  Id: Long,
                  PlayerSlot: Int,
                  RadiantWin: Boolean,
                  Duration: Int,
                  GameMode: Int,
                  LobbyType: Int,
                  HeroId: Int,

                )