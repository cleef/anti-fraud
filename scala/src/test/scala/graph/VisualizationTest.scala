package graph

import GraphApplication._
import redsea.LocalSparkContext


object VisualizationTest extends  LocalSparkContext  {

  def draw(min:Int, max:Int, columns:Seq[String]) = {
    val nodeGraph = GraphApplication.buildAndFilterGraph(trainData, true, min, max, columns:_*)

    val fileName =   "F:/anti-fraud/data/output/graph.png"
    Visualization.plot(nodeGraph, fileName)
    println(nodeGraph.vertices.count)
    println(nodeGraph.edges.count)
  }

  def main(args: Array[String]): Unit = {
    val columns = Seq("imsi", "mac")
    val rawGraph = draw(10, 10, columns)
  }

}
