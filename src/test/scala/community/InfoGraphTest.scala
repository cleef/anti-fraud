package community

import Infomap._
import InfoGraph._

class InfoGraphTest  extends TestBase {

  var infoGraph:InfoGraph = _
  var nodeSeq:Seq[Node] = _

  before {
    setup

    infoGraph = new InfoGraph(nodeNum, edges)
    nodeSeq = infoGraph.nodeSeq
  }

  test("codeLength") {
    var moduleIdx = infoGraph.nodeSeq.map(n => (n.idx, n.idx) ).toMap

    var partition = calcCodeLength(nodeSeq, moduleIdx)
    println(partition.codeLength)

    moduleIdx = load_module("data/Nets/simple.module1").toMap
    partition = calcCodeLength(nodeSeq, moduleIdx)
    println(partition.codeLength)
  }

  test("moveNode") {
    var moduleIdx = load_module("data/Nets/simple.module3").toMap

    var partition = calcCodeLength(nodeSeq, moduleIdx)
    val node = infoGraph.nodeMap(2)
    val moduleId = InfoGraph.moveNode(node, partition)
    println(moduleId)
  }

  test("run") {
    val partition = Infomap.run(infoGraph, 10)
    printPartition(partition)
    print(partition.codeLength)
  }
}
