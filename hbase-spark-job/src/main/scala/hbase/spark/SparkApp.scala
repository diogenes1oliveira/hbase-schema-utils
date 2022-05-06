package hbase.spark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

/**
 * App to store tracking requests from the output topic into HBase
 */
object SparkApp {
  val LOGGER = LogManager.getLogger(SparkApp.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hbase-spark-job")
      .getOrCreate()
    val df = spark.read.format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping","id STRING :key, col STRING f:col")
      .option("hbase.table", "testspark")
      .option("hbase.spark.use.hbasecontext", "false")
      .option("hbase.spark.pushdown.columnfilter", "false")
      .load()

    df.show(10)
  }

}
