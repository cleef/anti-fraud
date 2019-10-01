
package graph

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import redsea.{LocalSpark, LocalSparkContext}

class GraphApplication extends  LocalSparkContext  {
  import spark.implicits._
  import GraphApplication._

  def propFraudScore(trainData:String, propScoreSaveDir:String) = {
    // build graph
    val rawGraph:Graph[Node, Int] = buildGraph(trainData, true, attrColumns:_*)

    //filter by in degree
    val graph = GraphHelper.subGraphByInDegree(rawGraph, minInDegree, maxInDegree)
    val scoreGraph = graph.mapVertices( (vid: VertexId, deg: (Node, Int)) => deg._1 )

    // calc prop fscore
    val rank = BiPageRank.run(scoreGraph, 5)

    // _2._1._1 to get Node from ( vertexId, ( (Node, Int), Double) )
    val fraudScore = graph.vertices.join(rank.vertices).filter( row => !row._2._1._1.label.equals("user"))
    val result = fraudScore.map( row => (row._1, row._2._1._1.label, row._2._1._1.value, row._2._1._2, row._2._2 ) )
    val resultDf = result.toDF("id", "label", "value", "inDegree", "fraudScore").orderBy($"fraudScore".desc)

    LocalSpark.saveAsCsv(resultDf, propScoreSaveDir)
  }

  def userFraudScore(testData:String, propScoreData:String, userScoreSaveDir:String) = {
//    val df = GraphLoader.loadData(testData)
    val scoreDf  = LocalSpark.loadCsv(propScoreData).withColumn("id", col("id").cast("Long")).withColumn("fraudScore", col("fraudScore").cast("Double"))
    val score:RDD[(VertexId, Double)] = scoreDf.map( row => (row.getAs[Long]("id"), row.getAs[Double]("fraudScore")) ).rdd

    val graph:Graph[Node, Int] = buildGraph(testData, false, attrColumns:_*)
    val reverse = graph.reverse

    val scoreGraph = reverse.joinVertices(score)( (id: VertexId, node:Node, score:Double) => Node(node.label, node.value, node.flag, score)   )
    val rank = BiPageRank.run(scoreGraph, 5)

    // _2._2  to get score from ( vertexId, (Node , Double) )
    val fraudScore = graph.vertices.join(rank.vertices).filter( row => row._2._1.label.equals("user"))
    val resultDf = fraudScore.map( row => (row._1, row._2._2, row._2._1.flag) ).toDF("user_id", "fscore", "flag").orderBy($"fscore".desc)

    LocalSpark.saveAsCsv(resultDf, userScoreSaveDir)
  }
}

object GraphApplication extends  LocalSparkContext {

  val userIdColumn = "user_id"
  val flagColumn = "user_flag"
  val attrColumns = Seq(
    "zhuce_phone", "phone1",  "phone2",
    "company_name", "company_tel", "company_address",
    "mac", "imei", "imsi", "idfa", "ip")

  val minInDegree = 2
  val maxInDegree = 200

  val trainData = "file:///F:/anti-fraud/data/pagerank_property.csv"
  val testData = "file:///F:/anti-fraud/data/pagerank_user.csv"

  val propScoreSaveDir = "file:///F:/anti-fraud/data/prop_fscore"
  val userScoreSaveDir = "file:///F:/anti-fraud/data/user_fscore"
  val propScoreData = "file:///F:/anti-fraud/data/prop_fscore.csv"

  def buildGraph(data:String, train_mode: Boolean, attrColumns:String*) = {
    val df = GraphLoader.loadData(data)

    GraphLoader.train_mode = train_mode
    val graph:Graph[Node, Int] = GraphLoader.buildGraph(df, userIdColumn, flagColumn, attrColumns:_*)
    graph
  }

  def buildAndFilterGraph(data:String, train_mode: Boolean, min:Int, max:Int, attrColumns:String*): Graph[Node, Int] = {
    val rawGraph:Graph[Node, Int] =buildGraph(data, train_mode, attrColumns:_*)
    val graph = GraphHelper.subGraphByInDegree(rawGraph, min, max)
    val nodeGraph = graph.mapVertices( (vid: VertexId, deg: (Node, Int)) => deg._1 )

    nodeGraph
  }

}
