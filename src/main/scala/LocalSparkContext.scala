
package redsea

trait LocalSparkContext {
  System.setProperty("hadoop.home.dir","E:/redsea/data" )

  val spark = LocalSpark.getSpark()

  val sc = spark.sparkContext

}
