package dota.etl

import scala.util.{Failure, Success, Try}

object InputLoop {

  private def parseInput(input: String): Try[Integer] = Try(Integer.valueOf(input))

  // Input vars
  private var n = 0
  private var inputIsValid = false

  // Input loop

  /**
   * Starts the IO loop until a valid value is input
   */
  def start(): Unit = while (!inputIsValid) {
    val input = scala.io.StdIn.readLine("""
        |How many matches would you like to check?
        |(Or press enter for a default input of 10)
        |""".stripMargin)

    val result = handleInput(input)
    n = result._1
    inputIsValid = result._2
  }

  /**
   * Checks if the input is valid
   * @param input input to check
   * @return numeric value if possible and boolean validity flag
   */
  def handleInput(input: String): (Int, Boolean) = if (input == "") {
    (10, true)
  }
  else {
    parseInput(input).filter(x => 0 < x && x <= 20) match {
      case Success(value) => (value, true)
      case Failure(_) =>
        println("Input should be a number should be from 1 to 20 (both included)")
        (0, false)
    }
  }

  /**
   * Gets input
   * @return input
   */
  def getInput: Int = n
}
