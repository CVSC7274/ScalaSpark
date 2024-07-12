package pack

import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object salary {
  def main(args: Array[String]): Unit = {


    println("================Started1============")
    val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///F:/data/dept_new.csv")

    df.show(false)

    val sumdf = df.groupBy("dept").agg(sum("value") as "value")
    sumdf.show()

    val descdf= Window.orderBy(col("value").desc)

    sumdf.withColumn("dense_rank", dense_rank() over descdf)
      .withColumn("rank", rank() over descdf)
      .filter("dense_rank == 2")
      .show()

  }
}
