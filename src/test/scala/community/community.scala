package community

import org.apache.spark.graphx.VertexId


class CommnityTest  extends TestBase {

  test("load module") {
    val file = "data/Nets/simple.module"
    val result = load_module(file)
    result.foreach(println)
  }

  test("load edge") {
    val edges = load_edge("data/Nets/simple.net")
    println(edges.size)
  }

  test("pagerank") {
    val ranks: Map[VertexId, Double] = run_pagerank(edges, 20, 0.15)
    ranks.toSeq.sortBy( _._1 ).foreach(println)
  }
}
