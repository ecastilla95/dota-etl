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

object DotaETL{

  def main(args: Array[String]): Unit = {

    // Akka Actor System
    implicit val system: ActorSystem = ActorSystem()
    // needed for the future flatMap/onComplete in the end
    implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

    // https://api.opendota.com/api/players/639740/recentMatches
    val account_id = 639740
    val uri = s"https://api.opendota.com/api/players/${account_id}/recentMatches"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(GET, uri))

    Await.result(responseFuture, atMost = 5 seconds)

    responseFuture
      .onComplete {
        case Success(res) => res.status match {
          case StatusCodes.OK => println("poggers")
            val aaa: Future[HttpEntity.Strict] = res.entity.toStrict(5 seconds)
            aaa.map(_.data.utf8String).foreach(println)
          case _ => println("oof") // TODO
        }
        case Failure(_)   => sys.error("something wrong")
      }

    system.terminate()
  }
}
