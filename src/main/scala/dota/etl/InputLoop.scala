package dota.etl

import scala.util.{Failure, Success, Try}

object InputLoop {

  private def parseInput(input: String) = Try(Integer.valueOf(input))

  // Input vars
  private var n = 0
  private var inputIsValid = false

  // Input loop
  while (!inputIsValid) {
    val input = scala.io.StdIn.readLine("How many matches would you like to check?")
    if (input == "") {
      n = 10
      inputIsValid = true
    } else {
      parseInput(input).filter(x => 0 < x && x <= 20) match {
        case Success(value) =>
          n = value
          inputIsValid = true
        case Failure(exception) => println("Input should be a number should be from 1 to 20 (both included)")
      }
    }
  }

  def getInput: Int = n
}