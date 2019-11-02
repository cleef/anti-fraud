package graph

import redsea.{LocalSpark, LocalSparkContext}

object SaveTest extends  LocalSparkContext {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = Seq((1, "First Value", java.sql.Date.valueOf("2010-01-01")), (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))).toDF("int_column", "string_column", "date_column")

    val tmp = "file:///F:/anti-fraud/data/output"
    LocalSpark.saveAsCsv(df, tmp)
  }

}
