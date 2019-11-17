package graph

object ProjectionTest  {

  def main(args: Array[String]): Unit = {
    val columns = Seq("imsi")
    val graph = GraphApplication.buildAndFilterGraph(GraphApplication.trainData, true, 10, 10, columns:_* )

    Visualization.plot(graph, "F:/anti-fraud/scala/data/output/graph.png")

    println(graph.vertices.count)
    println(graph.edges.count)
    val userGraph = GraphHelper.projection(graph)

    println(userGraph.vertices.count)
    println(userGraph.edges.count)  // n * (n - 1) / 2

    GraphHelper.homophily(graph)

    Visualization.plot(userGraph, "F:/anti-fraud/scala/data/output/user_graph.png")
  }

}
