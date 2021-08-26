package com.aibyte.bigdata.spark.examples

import org.apache.hadoop.fs.Path

import java.net.URI

object SparkDistCPOptionsParsing {
  def parse(args: Array[String]):Config = {
    val parser = new scopt.OptionParser[Config]("") {
      opt[Unit]("i")
        .action((_,c) => c.copyOptions(_.copy(ignoreErrors = true)))
        .text("Ignore failures")

      opt[Int]("m")
        .action((i, c) => c.copyOptions(_.copy(maxConcurrence = i)))
        .text("max Concurrence")
      arg[String]("[source_path...] <target_path>").unbounded().minOccurs(2).action((u, c) => c.copy(URIs = c.URIs :+ new URI(u)))
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        config.options.validateOptions()
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}

case class Config(options: SparkDistCPOptions = SparkDistCPOptions(), URIs: Seq[URI] = Seq.empty) {
  def copyOptions(f: SparkDistCPOptions => SparkDistCPOptions): Config = {
    this.copy(options = f(options))
  }

  def sourceAndDestPaths: (Seq[Path], Path) = {
    URIs.reverse match {
      case d :: s :: ts => ((s :: ts).reverse.map(u => new Path(u)), new Path(d))
      case _ => throw new RuntimeException("Incorrect number of URIs")
    }
  }
}