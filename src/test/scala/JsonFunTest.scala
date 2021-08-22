import jsontest.JsonFun.Matches
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

class JsonFunTest extends AnyFunSuite {
  test("Json parsing") {

    // Implicits
    import jsontest.JsonFun.MatchesProtocol._

    val input = scala.io.Source.fromFile("src/test/resources/test.json")("UTF-8")
    val inputString = input.mkString.parseJson

    val matches = inputString.convertTo[Matches]

    println(matches)
  }
}
