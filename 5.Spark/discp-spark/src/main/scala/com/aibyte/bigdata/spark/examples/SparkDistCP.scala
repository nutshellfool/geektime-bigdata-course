package com.aibyte.bigdata.spark.examples

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkDistCP {

  /**
   * EntryPoint of DistCP cli, Arguments:
   *
   * Usage: sparkDistCP [options] [source_path] <target_path>
   * --i                Ignore failures
   * --m <value>        max concurrence of spark task
   *
   * @param args cli arguments
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate();
    val config = SparkDistCPOptionsParsing.parse(args)
    val (src, dest) = config.sourceAndDestPaths
    run(sparkSession, src, dest, config.options)
  }

  def run(sparkSession: SparkSession, sourcePaths: Seq[Path], destinationPath: Path, sparkDistCPOptions: SparkDistCPOptions): Unit = {
    checkDir(sourcePaths, destinationPath)
    copyFiles(("",""), sparkSession.sparkContext)
  }

  def checkDir(sourcePaths: Seq[Path], destinationPath: Path): Unit = {
    // TODO
  }

  def copyFiles(fileList: (String, String), sparkContext: SparkContext): Unit = {
    //

  }


}
