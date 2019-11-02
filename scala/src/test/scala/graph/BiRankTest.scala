package graph

import redsea.LocalSparkContext
import graph.{BiPageRank, Node}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


object BiRankTest extends  LocalSparkContext {

  def main(args: Array[String]): Unit = {
    // no indegree, no message
    val u = Node("user", "1", 0, 0)
    val imei = Node("imei", "imei1", 0, 0)
    val imsi = Node("imsi", "imsi1", 0, 0)

    val nodes: RDD[(VertexId, Node)] = sc.parallelize(Array((1L, u), (2L, imei), (3L, imsi) ))

    val relationships: RDD[Edge[Int]] = sc.parallelize( Array(Edge(2L, 1L, 0),    Edge(3L, 1L, 0)) )

    val graph = Graph(nodes, relationships)
    val rank = BiPageRank.run(graph, 4)

    rank.vertices.foreach(println)
  }

}
