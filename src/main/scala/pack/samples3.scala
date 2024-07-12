package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object samples3 {
  def main(args: Array[String]): Unit = {

    println("================Started============")
    val conf = new SparkConf().setAppName("revision")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    val df= spark.read.format("csv").option("header", "true").load("s3://csprodbuck/srcdata/")
    println("================ raw data ============")
    df.show()

    val df1=df.filter(col("category") === "Gymnastics")

    df1.write.format("parquet").mode("overwrite").save("s3://csprodbuck/resdata/")
    println("================ filtered data ============")
    df1.show()

    println("================completed============")

  }

  }
