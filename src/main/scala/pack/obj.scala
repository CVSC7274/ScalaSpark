package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._


object obj {

  def main(args: Array[String]): Unit = {


    println("================Started1============")
    val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    println("================ snow df ============")

    val snowdata = spark.read.format("snowflake")
      .option("sfURL", "https://odlovje-mi77519.snowflakecomputing.com")
      .option("sfAccount", "MI77519")
      .option("sfUser", "CSANALYTICS1")
      .option("sfPassword", "Snowflake@1#1")
      .option("sfDatabase", "csdb")
      .option("sfSchema", "csschema")
      .option("sfRole", "ACCOUNTADMIN")
      .option("query", """select * from csdb.csschema.session_details """)
      .load()

    snowdata.show(false)

    println("================ snow updated ============")

    val snowdf = snowdata
      .withColumn("value", expr("concat(value, '}')"))
    snowdf.show(false)

    val minbatchid = snowdf.agg(
      min("batchid").as("minId")
    )
    minbatchid.show()
    val minId = minbatchid.head().get(0)

    val maxbatchid = snowdf.agg(
      max("batchid").as("maxId")
    )
    maxbatchid.show()
    val maxId = maxbatchid.head().get(0)

    println("================ yesterday data ============")
    val yesdf = snowdf.filter(col("batchid") === minId)
      .withColumnRenamed("value", "Yvalue")
    yesdf.show(false)

    println("================ today data ============")
    val todaydf = snowdf.filter(col("batchid") === maxId)
      .withColumnRenamed("value", "Tvalue")
    todaydf.show(false)

    println("================ missing data ============")
    val missingdf = yesdf.join(todaydf, Seq("id"), "leftanti")
    missingdf.show(false)

    println("================ new data ============")
    val newdf = todaydf.join(yesdf, Seq("id"), "leftanti")
    newdf.show(false)

    val commondf = yesdf.join(todaydf, Seq("id"), "inner")
      .drop("batchid")

    val compdf = commondf
      .withColumn("result", expr(" case when Yvalue==Tvalue then true else false end"))


    println("================ updated data ============")
    val updatedf = compdf.filter(col("result") === "false").drop("result")
    updatedf.show(false)

    println("================ similar data ============")
    val samedf = compdf.filter(col("result") === "true").drop("result")
    samedf.show(false)

    println("================ scenario completed ============")

  }

}