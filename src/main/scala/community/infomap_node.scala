package community

import org.apache.spark.graphx.VertexId


case class Node(idx:VertexId, size: Double, teleport: Double, fromWeight: Seq[(VertexId, Double)], toWeight: Seq[(VertexId, Double)])

case class Module(idx:ModuleId, nodeNum:Int, size:Double, teleport: Double, exit: Double, exitWeight:Double)

case class Partition(moduleIdx: collection.mutable.Map[VertexId, ModuleId],
                     moduleMap:collection.mutable.Map[ModuleId, Module],
                     var codeLength:Double,
                     var qsum:Double)

case class Move(idx:VertexId, delta:Double, qsum:Double, new_from: Module, new_to: Module)

case class Merge(delta:Double, qsum:Double, from:ModuleId, new_to: Module)

