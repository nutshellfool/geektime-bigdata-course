package com.aibyte.bigdata.spark.examples

import org.apache.spark.sql.SparkSession

object SparkRDDInvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("lirui-SparkCoreRDDInvertedIndex")
      .getOrCreate()

    // TODO: read input file from args[0] (hdfs path)
    val rdd = spark.sparkContext.wholeTextFiles("file:///Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/src/main/resources/text/*.txt")
    rdd.flatMap {
      case (path, text) =>
        text.split("""\W+""").map {
          word => (word, path)
        }
    }.map({
      case (word, path) => ((word, path.split("/").takeRight(1)(0)), 1)
    }).reduceByKey({
      case (n1, n2) => n1 + n2
    }).map({
      case ((word, path), number) => (word, (path, number))
    }).groupBy({
      case (word, (path, number)) => word
    }).map({
      case (word, seq) => seq.mkString(",")
    })// TODO : write output by arg[1] (hdfs path)
      .saveAsTextFile("file:///Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/target/output.txt")
  }

}
