import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object Main {
  def main(args : Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()



    //tableau
    val rdd1 = sparkSession.sparkContext.textFile("data/donnees.csv")
    rdd1.foreach(println)


  }

}