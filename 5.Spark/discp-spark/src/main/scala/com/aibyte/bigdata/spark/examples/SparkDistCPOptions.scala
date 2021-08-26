package com.aibyte.bigdata.spark.examples

case class SparkDistCPOptions(ignoreErrors: Boolean = SparkDistCPOptions.defaults.ignoreError,
                              maxConcurrence: Int = SparkDistCPOptions.defaults.maxConcurrence,
                             ) {

  def validateOptions(): Unit = {
    assert(maxConcurrence > 0, "max concurrence must positive")
  }

}

object SparkDistCPOptions {
  object defaults {
    val ignoreError: Boolean = false
    val maxConcurrence: Int = 5
  }
}