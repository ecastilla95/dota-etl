import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object DotaETL extends Directives with SprayJsonSupport {

  def main(args: Array[String]): Unit = {

    // Akka Actor System
    implicit val system: ActorSystem = ActorSystem()

    // needed for the future flatMap/onComplete in the end
    implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

    // Json Parser
    implicit val MatchFormat = jsonFormat6(Match)

    // https://api.opendota.com/api/players/639740/recentMatches
    val account_id = 639740
    val uri = s"https://api.opendota.com/api/players/${account_id}/recentMatches"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(GET, uri))

    //    Await.result(responseFuture, atMost = 5 seconds)

    println("Stop 1")
    val result: Future[Either[ResponseEntity, String]] = responseFuture.map { res: HttpResponse =>
      res.status match {
        case StatusCodes.OK =>
//          val dotaMatch: Future[Match] = Unmarshal[ResponseEntity](res.entity.withContentType(ContentTypes.`application/json`)).to[Match]
//          Await.ready(dotaMatch, Duration.Inf)
//          dotaMatch.value.flatMap(_.toOption) match {
//            case Some(x) => Left(x)
//            case None => Right("parse error")
//          }
          Left(res.entity.withContentType(ContentTypes.`application/json`))
        case _ => Right("http error")
      }
    }

    println("Stop 2")
    Await.ready(result, Duration.Inf)
    result.onComplete {
      case Success(value) => println(value)
      case Failure(_) => println(Right("connection error"))
    }

    println("Stop 3")
    system.terminate()
  }
}
