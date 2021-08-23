import dota.etl.InputLoop
import org.scalatest.funsuite.AnyFunSuite

class InputLoopTest extends AnyFunSuite {

  test("handeInput test") {

    val inputs = Seq("", "0", "1", "19", "20", "21", "NaN")
    val actual = inputs.map(InputLoop.handleInput)
    val expected = Seq(
      (10, true),
      (0, false),
      (1, true),
      (19, true),
      (20, true),
      (0, false),
      (0, false)
    )
    assert(actual == expected)
  }

}
