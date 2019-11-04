
package redsea

trait LocalSparkContext {
  // $dir$/bin/winutils.exe
  System.setProperty("hadoop.home.dir","F:/anti-fraud/hadoop_home" )

  val spark = LocalSpark.getSpark()

  val sc = spark.sparkContext

}
