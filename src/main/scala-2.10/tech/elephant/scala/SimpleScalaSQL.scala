package tech.elephant.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import grizzled.slf4j.Logger

object SimpleScalaSQL {
  val logger = Logger[this.type]

  def main(args: Array[String]) {
    // A parquet file on hdfs
    val conf = new SparkConf().setAppName("Simple SQL Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//    val parquetPath = "/user/oliver/hdfs_export/occurrence_parquet_full"
//    val parquetPath = "/user/hive/warehouse/passer_parquet"
    val parquetPath = "/user/hive/warehouse/peregrine_parquet"
    val parquetFile = sqlContext.parquetFile(parquetPath)
    val peregrine1 = sqlContext.parquetFile(parquetPath)
    val peregrine2 = sqlContext.parquetFile(parquetPath)
    import sqlContext.implicits._

    parquetFile.cache()
    val start1 = System.currentTimeMillis()
    val count1 = parquetFile.count()
    val stop1 = System.currentTimeMillis()
    val time1 = (stop1 - start1) / 1000
    logger.info("Time for first count of " + count1 + ": " + (stop1-start1))

    val start2 = System.currentTimeMillis()
    val count2 = parquetFile.count()
    val stop2 = System.currentTimeMillis()
    val time2 = (stop2-start2)/1000
    logger.info("Time for second count of " + count2 + ": " + (stop2 - start2))

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("occurrence_parquet")

    val kingdoms = sqlContext.sql("SELECT distinct(kingdom) FROM occurrence_parquet")
    val kingdomStrings = kingdoms.map(t => "Kingdom: " + t(0)).collect()
//    for (kingdom <- kingdomStrings) logger.info(kingdom)

    val kingdomCounts = sqlContext.sql("SELECT kingdom, count(*) FROM occurrence_parquet GROUP BY kingdom")
    val kingdomStrings = kingdoms.map(t => "Kingdom: " + t(0)).collect()
//    for (kingdom <- kingdomStrings) logger.info(kingdom)

    val taxa = sqlContext.sql("SELECT taxonkey, count(*) FROM occurrence_parquet group by taxonkey")
    val taxaCounts = taxa.map(t => "Taxon: " + t(0) + " count: " + t(1)).collect()

//    val counts = firstLatLng.map(t => "" + t(1) + "\t" + t(2) + "\t" + t(3) + "\t" + t(4) + "\t" + t(5)).collect()


    //    val df = sqlContext.load(logFile)
//    df.registerTempTable("spark_occurrence_parquet")
//    val animalia = sqlContext.sql("select count(*) where kingdom='Animalia' from spark_occurrence_parquet")
//    logger.info("Lines with Animalia: [" + animalia + "]")

//    val logData = sc.textFile(logFile, 2).cache()
//    //    val numAs = logData.filter(line => line.contains("a")).count()
//    //    val numBs = logData.filter(line => line.contains("b")).count()
//    val animalia = logData.filter(line => line.contains("Animalia")).count()
//    val plantae = logData.filter(line => line.contains("Plantae")).count()
//    //    logger.info("Lines with a: [" + numAs + "], Lines with b: [" + numBs + "]")
//    logger.info("Lines with Animalia: [" + animalia + "], Lines with Plantae: [" + plantae + "]")
  }
}
