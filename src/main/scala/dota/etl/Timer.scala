package dota.etl

import com.typesafe.scalalogging.Logger

object Timer {

  /**
   * Times an execution block
   * @param f execution block
   * @tparam A return type of f
   * @return
   */
  def time[A](f: => A)(implicit logger: Logger): A = {
    val t0 = System.nanoTime()
    val result = f
    val t1 = System.nanoTime()
    logger.info("Elapsed time: " + (t1 - t0) + "ns")
    result

  }

}
