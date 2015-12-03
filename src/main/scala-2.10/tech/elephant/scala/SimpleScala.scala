package tech.elephant.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import grizzled.slf4j.Logger
import org.apache.spark.sql.SQLContext

object SimpleScala {
  val logger = Logger[this.type]

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val test = sqlContext.read.json("/Users/oliver/SourceCode/git/scala-games/src/main/resources/test.json")
    println(test)


  }

//  class MatchFunctions(val line: List) {
//    def isMatch(s: )
//  }
}
