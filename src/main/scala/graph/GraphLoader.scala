package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import redsea.{LocalSpark, LocalSparkContext}

import scala.util.hashing.MurmurHash3

object GraphLoader extends LocalSparkContext{

  import spark.implicits._

  var train_mode = true

  def loadData(dataPath:String) = {
    val df = LocalSpark.loadCsv(spark, dataPath)

    df.withColumn("user_id", col("uid").cast("Long"))
      .withColumn("user_flag", col("user_flag").cast("Int"))
  }

  def saveDegree(graph:Graph[Node, Int], dataPath:String) = {
    val inDegree = GraphHelper.joinInDegree(graph)
    val resultDf = inDegree.vertices.map( r => (r._1, r._2._1.label, r._2._1.value, r._2._2)  ).toDF("id","label", "value", "inDegree")

    val df = resultDf.filter("inDegree > 1").orderBy($"inDegree".desc)
    LocalSpark.saveAsCsv(df, dataPath)
}

  def buildEdge(row:Row, userIdColumn:String, attrColumns:String*) = {
    val userId = row.getAs[Long](userIdColumn)
    for {
      attrColumn <- attrColumns
      value = row.getAs[String](attrColumn)
      if (value != null)
    } yield Edge(userId, MurmurHash3.stringHash(value).toLong, 0) // Graph.fromEdge need a ED
  }

  def buildNode(row:Row, userIdColumn:String, flagColumn:String, attrColumns:String*): Seq[(VertexId, Node)] = {
    var result = (
      for {
        attrColumn <- attrColumns
        value = row.getAs[String](attrColumn)
        if (value != null)
      } yield (MurmurHash3.stringHash(value).toLong, Node(attrColumn, value, -1, 0))
      )

    val userId = row.getAs[Long](userIdColumn)
    val flag = row.getAs[Int](flagColumn)

    //init score
    val score =  if(train_mode) flag2score(flag) else 0
    val userNode = (userId, Node("user", userId.toString, flag, score))

    result :+ userNode
  }

  def flag2score(flag:Int) = {
    flag match {
      case Node.flag_bad => 1.0
      case Node.flag_reject => 0.5
      case _ => 0.0
    }
  }

  def buildGraph(df:DataFrame, userIdColumn:String, flagColumn:String, attrColumns:String*) = {
    val edgeDf = df.flatMap( row => buildEdge(row, userIdColumn, attrColumns:_*) )
    val node = df.rdd.flatMap( row => buildNode(row, userIdColumn, flagColumn, attrColumns:_*))

    Graph(node, edgeDf.rdd)
  }
}
