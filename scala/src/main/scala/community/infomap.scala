package community

import scala.util.Random

object Infomap {

  def run(graph: InfoGraph, numIter: Int):Partition = {
    val nodeSeq = graph.nodeSeq
    val nodeMap = graph.nodeMap

    var partition = graph.partition

    var iteration = 0
    var running = true
    val nodeId = nodeSeq.map(n => n.idx)
    while (iteration < numIter & running) {
      var hasChange = false

      //single node move
      val perm = Random.shuffle(nodeId)
      for (nodeId <- perm) {
        val node = nodeMap(nodeId)
        val move = InfoGraph.moveNode(node, partition)

        move match {
          case Some(m) => {
            graph.applyMove(m)

            hasChange = true
          }
          case None =>
        }
      }

      //module merge
      val merge = InfoGraph.mergeModule(graph, partition)
      merge match {
        case Some(m) => {
          graph.applyMerge(m)
          hasChange = true
        }
        case None =>
      }

      running = hasChange
      iteration = iteration + 1
      println(iteration + " ---------------")
    }

    partition
  }



  def printPartition(partition: Partition) = {
    val group = partition.moduleIdx.toSeq.groupBy { case (vid, mid) => mid }
    val sortGroup = group.map {
      case (mid, seq) => (mid, seq.map {
        case (vid, mid) => vid
      }.sorted)
    }.toSeq.sortBy(_._1)

    sortGroup.foreach(println)
    partition.moduleMap.toSeq.sortBy(_._1).foreach(println)
  }
}

