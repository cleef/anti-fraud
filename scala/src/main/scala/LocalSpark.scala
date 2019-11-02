package redsea

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocalSpark {

  def loadCsv(spark: SparkSession, dataPath:String) = {
    val df = spark.read
      .option("header", true)
      .csv(s"${dataPath}")

    df
  }

  def saveAsCsv(df:DataFrame, path:String) = {
    df.coalesce(1).write.option("header", "true").format("csv").save(path)
  }

  def getSpark(): SparkSession = {
    val conf = new SparkConf().setAppName("local-spark").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    spark.sparkContext.setCheckpointDir("/checkpoint")

    spark
  }

}
