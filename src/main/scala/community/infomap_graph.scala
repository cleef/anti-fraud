package community

import org.apache.spark.graphx.VertexId

class InfoGraph(nodeNum: Int, edges: Seq[((VertexId, VertexId), Double)]) {
  import InfoGraph._

  val nodeSeq: Seq[Node] = {
    require(edges.size > 0, "edges size > 0")

    //node size
    val node_size: Map[VertexId, Double] = calcNodeSize(edges)

    //links
    val outlink: Map[VertexId, Seq[((VertexId, VertexId), Double)]] = edges.groupBy { case ((from, to), _) => from }
    val inlink: Map[VertexId, Seq[((VertexId, VertexId), Double)]] = edges.groupBy { case ((from, to), _) => to }

    val node_outsum = outlink.map {
      case (from, seq) => (from, seq.map {
        case ((from, to), weight) => weight
      }.sum)
    }

    val node_outlink = outlink.map {
      case (from, seq) => (from, seq.map {
        case ((from, to), weight) => (to, weight / node_outsum(from) * beta * node_size(from))
      })
    }

    val node_inlink = inlink.map {
      case (to, seq) => (to, seq.map {
        case ((from, to), weight) => (from, weight / node_outsum(from) * beta * node_size(from))
      })
    }

    val teleport = 1.0 / nodeNum
    val nodeSeq = node_size.map { case (vid, size) =>
      var outlink: Seq[(VertexId, Double)] = Seq.empty
      var inlink: Seq[(VertexId, Double)] = Seq.empty
      if (node_outlink.contains(vid))
        outlink = node_outlink(vid)
      if (node_inlink.contains(vid))
        inlink = node_inlink(vid)
      Node(vid, size, teleport, outlink, inlink)
    }.toSeq.sortBy(_.idx)

    nodeSeq
  }

  val nodeMap: Map[VertexId, Node] = {
    nodeSeq.map(n => (n.idx, n)).toMap
  }

  var partition = {
    calcCodeLength(nodeSeq)
  }

  def applyMove(move:Move) = {
    val moduleIdx = partition.moduleIdx
    val moduleMap = partition.moduleMap
    partition.codeLength = partition.codeLength + move.delta
    partition.qsum = move.qsum

    //flip node
    val nodeId = move.idx
    val from = move.new_from
    val to = move.new_to
    moduleIdx(nodeId) = to.idx

    //change module
    moduleMap(to.idx) = to
    if(from.nodeNum > 0)
      moduleMap(from.idx) = from
    else
      moduleMap.remove(from.idx)
  }

  def applyMerge(merge:Merge) = {

    val moduleIdx = partition.moduleIdx
    val moduleMap = partition.moduleMap
    partition.codeLength = partition.codeLength + merge.delta
    partition.qsum = merge.qsum

    val from = merge.from
    val to = merge.new_to
    //flip node
    nodeSeq.view.filter(n => moduleIdx(n.idx) == from).foreach(n => moduleIdx(n.idx) = to.idx)
    //change module
    moduleMap(to.idx) = to
    moduleMap.remove(from)

  }

}

object InfoGraph {

  def apply(edges: Seq[((VertexId, VertexId), Double)]) = {
    val nodeNum = (edges.map { case ((from, to), _) => from } ++ edges.map { case ((from, to), _) => to }).toSet.size

    new InfoGraph(nodeNum, edges)
  }

  /**
    * single node move
    *
    * @param node
    * @param partition
    * @return
    */
  def moveNode(node: Node, partition: Partition): Option[Move] = {
    val moduleIdx = partition.moduleIdx
    val fromWeight = node.fromWeight.map { case (to, weight) => (moduleIdx(to), weight) }
    val toWeight = node.toWeight.map { case (from, weight) => (moduleIdx(from), weight) }

    val candidates = fromWeight.map { case (mid, weight) => mid }.toSet ++ toWeight.map { case (mid, weight) => mid }.toSet
    val from = moduleIdx(node.idx)
    val moveSeq: Seq[Move] = candidates.view.filter(to => to != from).map { to =>
      {
        val move = moveNodeToNbr(node, fromWeight, toWeight, from, to, partition)
        move
      }
    }.toSeq.filter( _.delta < 0)

    moveSeq match {
      case moveSeq if moveSeq.size > 0 => Some(moveSeq.minBy(_.delta))
      case _ => None
    }
  }

  /**
    * try move node to neigbour module
    *
    * @param node
    * @param fromWeight
    * @param toWeight
    * @param from
    * @param to
    * @param partition
    * @return
    */
  def moveNodeToNbr(node: Node, fromWeight: Seq[(ModuleId, Double)], toWeight: Seq[(ModuleId, Double)], from: VertexId, to: VertexId, partition: Partition): Move = {
    val i = partition.moduleMap(to)
    val i_size = i.size + node.size
    val i_teleport = i.teleport + node.teleport
    val i_exitWeight = i.exitWeight +
      fromWeight.view.filter { case (mid, weight) => mid != to }.map { case (mid, weight) => weight }.sum -
      toWeight.view.filter { case (mid, weight) => mid == to }.map { case (mid, weight) => weight }.sum
    val i_exit = alpha * (1 - i_teleport) * i_size + i_exitWeight
    val new_to = Module(to, i.nodeNum + 1, i_size, i_teleport, i_exit, i_exitWeight)

    val j = partition.moduleMap(from)
    val j_size = j.size - node.size
    val j_teleport = j.teleport - node.teleport
    val j_exitWeight = j.exitWeight -
      fromWeight.view.filter { case (mid, weight) => mid != from }.map { case (mid, weight) => weight }.sum +
      toWeight.view.filter { case (mid, weight) => mid == from }.map { case (mid, weight) => weight }.sum
    val j_exit = alpha * (1 - j_teleport) * j_size + j_exitWeight
    val new_from = Module(from, j.nodeNum - 1, j_size, j_teleport, j_exit, j_exitWeight)

    val old_qsum = partition.qsum
    var (q_sum, delta) = j_size match {
      case 0 => {
        // empty module
        require( j_exit.abs < 1e-16, "empty module exit " + from + " " + j_exit)
        val q_sum = i_exit + j_exit - i.exit - j.exit + old_qsum
        val delta = plogp(q_sum) - plogp(old_qsum) -
          2 * plogp(i_exit) + 2 * plogp(i.exit) + 2 * plogp(j.exit) +
          plogp(i_exit + i_size) - plogp(i.exit + i.size) - plogp(j.exit + j.size)

        (q_sum, delta)
      }
      case _ => {
        val q_sum = i_exit + j_exit - i.exit - j.exit + old_qsum
        val delta = plogp(q_sum) - plogp(old_qsum) -
          2 * plogp(i_exit) - 2 * plogp(j_exit) + 2 * plogp(i.exit) + 2 * plogp(j.exit) +
          plogp(i_exit + i_size) + plogp(j_exit + j_size) - plogp(i.exit + i.size) - plogp(j.exit + j.size)

        (q_sum, delta)
      }
    }

    Move(node.idx, delta / ln2, q_sum, new_from, new_to)
  }

  def mergeModule(infoGraph: InfoGraph, partition: Partition): Option[Merge] = {
    // edge: node ->  node   to  module -> module
    // FIXME change to module to module
    val moduleIdx = partition.moduleIdx
    val moduleEdge: Map[(ModuleId, ModuleId), Double] = infoGraph.nodeSeq.flatMap { n =>
      n.toWeight.map {
        case (to, weight) => ((moduleIdx(n.idx), moduleIdx(to)), weight)
      }
    }.reduceByKey().filter { case ((from, to), weight) =>
      from != to
    }

    // get neighbour module
    val mergeSeq = moduleEdge.map { case ((from, to), weight) => mergeModuleToNbr(from, to, moduleEdge, partition) }.filter( _.delta < 0)

    mergeSeq match {
      case mergeSeq if mergeSeq.size > 0 => Some(mergeSeq.minBy(_.delta))
      case _ => None
    }
  }

  def mergeModuleToNbr(firstMid: ModuleId, secondMid: ModuleId, moduleEdge: Map[(ModuleId, ModuleId), Double], partition: Partition): Merge = {
    val i = partition.moduleMap(firstMid)
    val j = partition.moduleMap(secondMid)

    val i_j_weight = if (moduleEdge.contains((firstMid, secondMid))) moduleEdge((firstMid, secondMid)) else 0
    val j_i_weight = if (moduleEdge.contains((secondMid, firstMid))) moduleEdge((secondMid, firstMid)) else 0

    val k_size = i.size + j.size
    val k_teleport = i.teleport + j.teleport
    val k_exitWeight = i.exitWeight + j.exitWeight - i_j_weight - j_i_weight
    val k_exit = alpha * (1 - k_teleport) * k_size + k_exitWeight

    val q_sum = k_exit - i.exit - j.exit + partition.qsum
    val delta = plogp(q_sum) - plogp(partition.qsum) -
      2 * plogp(k_exit) + 2 * plogp(i.exit) + 2 * plogp(j.exit) +
      plogp(k_exit + k_size) - plogp(i.exit + i.size) - plogp(j.exit + j.size)

    val i_nodeNum = i.nodeNum
    val j_nodeNum = j.nodeNum
    val (from, to) = (i_nodeNum, j_nodeNum) match {
      case (i_nodeNum, j_nodeNum) if i_nodeNum < j_nodeNum => (firstMid, secondMid) //from -> to
      case (i_nodeNum, j_nodeNum) if i_nodeNum > j_nodeNum => (secondMid, firstMid)
      case _ => (Math.max(firstMid, secondMid), Math.min(firstMid, secondMid))
    }

    val new_to = Module(to, i_nodeNum + j_nodeNum, k_size, k_teleport, k_exit, k_exitWeight)
    Merge(delta / ln2, q_sum, from, new_to)
  }

  def calcNodeSize(edges: Seq[((VertexId, VertexId), Double)]): Map[VertexId, Double] = {
    var rank: Map[VertexId, Double] = run_pagerank(edges)

    rank
  }

  def calcCodeLength(nodeSeq: Seq[Node]): Partition = {
    var partition = calcCodeLength(nodeSeq, nodeSeq.map(n => (n.idx, n.idx)).toMap)
    partition
  }

  /**
    */
  def calcCodeLength(nodeSeq: Seq[Node], moduleIdx: Map[VertexId, VertexId]): Partition = {
    //module
    val module_nodes: Seq[(VertexId, Node)] = nodeSeq.map(n => (moduleIdx(n.idx), n))

    //module attribute
    val mod_node_num: Map[VertexId, Int] = module_nodes.map { case (mid, node) => (mid, 1) }.reduceByKey
    val mod_size: Map[VertexId, Double] = module_nodes.map { case (mid, node) => (mid, node.size) }.reduceByKey
    val mod_teleport_weight = module_nodes.map { case (mid, node) => (mid, node.teleport) }.reduceByKey
    val mod_exit_weight = module_nodes.flatMap {
      case (mid, node) => node.fromWeight.map {
        case (to, weight) => (mid, moduleIdx(to), weight)
      }
    }.view.filter {
      case (mid_1, mid_2, weight) => mid_1 != mid_2
    }.map {
      case (mid_1, mid_2, weight) => (mid_1, weight)
    }.reduceByKey

    // mod_exit = 0.15 * (1- mod_teleport_weight) * mod_size + mod_exit_weight
    val mod_info: Seq[Module] = mod_size.map {
      case (mid, size) =>
        val teleport = mod_teleport_weight(mid)
        var exit_weight = 0.0
        if (mod_exit_weight.contains(mid))
          exit_weight = mod_exit_weight(mid)
        val exit = alpha * (1 - teleport) * size + exit_weight
        Module(mid, mod_node_num(mid), size, teleport, exit, exit_weight)
    }.toSeq

    // module: sum( q * log(q) )
    val exit_log_exit = mod_info.map(n => plogp(n.exit)).sum
    //module: sum(q) * log( sum(q) )
    val qsum = mod_info.map(n => n.exit).sum
    val exit = plogp(qsum)
    //module: sum( p * log(p) )
    val size_log_size = mod_info.map(n => plogp(n.size + n.exit)).sum

    val nodeSize_log_nodeSize = nodeSeq.map(n => plogp(n.size)).sum / ln2
    val codeLength = (exit - 2.0 * exit_log_exit + size_log_size) / ln2 - nodeSize_log_nodeSize
    val moduleSeq = mod_info.map(m => (m.idx, m)).toSeq

    Partition(collection.mutable.Map(moduleIdx.toSeq:_*),  collection.mutable.Map(moduleSeq:_*) , codeLength, qsum)
  }
}

