package com.aibyte.bigdata.spark.examples

import org.apache.spark.sql.SparkSession

object SparkRDDInvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("lirui-SparkCoreRDDInvertedIndex")
      .getOrCreate()

    if (args == null || args.length < 2) {
      println("==== wrong parameters ====")
      println("usage: spark-submit " +
        "--class com.aibyte.bigdata.spark.examples.SparkRDDInvertedIndex " +
        "--master yarn-client(local) RDD-inverted-index-1.0-SNAPSHOT.jar " +
        "<input-file-path> " +
        "<output-file-path>")
      return
    }

    //  localEnvPath : "file:///Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/src/main/resources/text/*.txt"
    val path = args(0)
    // localEnvPath :  "file:///Users/RichardLee/Workspace/GrowingHack/geektime-bigdata-course/5.Spark/RDD-inverted-index/target/output"
    val outputFilePath = args(1)

    //  read inputFiles and creat (word, path) tuple
    val rdd = spark.sparkContext.wholeTextFiles(path)
    rdd.flatMap({
      case (path, text) =>
        text.split("""\W+""").map {
          word => (word, path)
        }
    }).map({
      // short the long path to short, and transform (word, path) to ((word, path), 1)
      case (word, path) => ((word, path.split("/").takeRight(1)(0)), 1)
    }).reduceByKey({
      // group all (word, path) pairs and sum count
      case (n1, n2) => n1 + n2
    }).map({
      // transform ((word, path), count) to (word, (path, count))
      case ((word, path), number) => (word, (path, number))
    }).groupBy({
      // group by word
      case (word, _) => word
    }).map({
      // format the out put
      case (_, seq) => seq.mkString(",")
    }).saveAsTextFile(outputFilePath)
  }
}
