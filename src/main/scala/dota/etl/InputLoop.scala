package dota.etl

import scala.util.{Failure, Success, Try}

object InputLoop {

  private def parseInput(input: String) = Try(Integer.valueOf(input))

  // Input vars
  private var n = 0
  private var inputIsValid = false

  // Input loop
  def start(): Unit = while (!inputIsValid) {
    val input = scala.io.StdIn.readLine("""
        |How many matches would you like to check?
        |(Or press enter for a default input of 10)
        |""".stripMargin)

    val result = handleInput(input)
    n = result._1
    inputIsValid = result._2
  }

  def handleInput(input: String): (Int, Boolean) = if (input == "") (10, true)
    else {
      parseInput(input).filter(x => 0 < x && x <= 20) match {
        case Success(value) => (value, true)
        case Failure(_) =>
          println("Input should be a number should be from 1 to 20 (both included)")
          (0, false)
      }
    }

  def getInput: Int = n
}