package jsontest

import spray.json._

object JsonFun {

  case class Match(
                    Id: Long,
                    PlayerSlot: Short,
                    RadiantWin: Boolean,
                    Kills: Short,
                    Deaths: Short,
                    Assists: Short
                  )
  case class Matches(items: List[Match])

  object MatchesProtocol extends DefaultJsonProtocol {
    implicit val matchFormat: RootJsonFormat[Match] = jsonFormat6(Match)
    implicit object friendListJsonFormat extends RootJsonFormat[Matches] {
      def read(value: JsValue) = Matches(value.convertTo[List[Match]])
      def write(f: Matches) = ???
    }
  }
}    