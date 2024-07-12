package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._

object cas {
  def main(args:Array[String]):Unit={

    val conf = new SparkConf()
      .setAppName("first")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val df = spark.read.format("org.apache.spark.sql.cassandra")
      .option("keyspace", "csdb1_new")
      .option("table", "emp_date")
      .load()

    df.show()
  }
}
