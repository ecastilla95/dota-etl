package dota.etl

object Timer {

  /**
   * Times an execution block
   * @param f execution block
   * @tparam A return type of f
   * @return
   */
  def time[A](f: => A): A = {
    val t0 = System.nanoTime()
    val result = f
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result

  }

}
