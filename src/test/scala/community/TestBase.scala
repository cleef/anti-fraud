package community

import org.apache.spark.graphx.VertexId
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestBase extends FunSuite with BeforeAndAfter {
  var nodeNum:Int = _
  var edges: Seq[((VertexId, VertexId), Double)] = _

  def setup() =  {
    edges = load_edge("data/Nets/simple.net")
    nodeNum =
      (edges.map{ case ((from, to), weight) => from } ++ edges.map{ case ((from, to), weight) => to }).toSet.size

    println("init edges")
  }

}
