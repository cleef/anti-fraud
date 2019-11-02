//package redsea.community

import java.io.File

import org.apache.spark.graphx.{VertexId}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.ClassTag


package object community {
  type ModuleId = VertexId

  val alpha = 0.15
  val beta = 1 - alpha

  val ln2 = Math.log(2)

  def plogp(p: Double) = p * Math.log(p)

  def load_module(moduleFile: String) = {
    var result = Seq[(Long, Long)]()
    for (line <- Source.fromFile(moduleFile).getLines()) {
      val idx = line.indexOf(" ")
      val moduleId = line.substring(0, idx).toLong
      val seq = line.substring(idx + 1).split(",").toSeq

      val lineSeq = seq.map(e => (e.trim.toLong, moduleId))
      result = result ++ lineSeq
    }

    result
  }

  /**
    *
    * github PajekReader
    *
    * @param edgeFile
    */
  def load_edge(edgeFile: String): Seq[((VertexId, VertexId), Double)] = {

    var edges = new ListBuffer[((Long, Long), Double)]()

    val starRegex = """\*([a-zA-Z]+).*""".r
    val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
    val edgeRegex1 = """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]*""".r
    val edgeRegex2 = """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]+([0-9.eE\-\+]+).*""".r


    var section: String = "__begin"
    var nodeNumber: Long = -1
    var lineNumber = 1 // line number in file, used when printing file error
    // read file serially
    for (line <- Source.fromFile(new File(edgeFile), "utf-8").getLines
         if (line != null && !line.isEmpty // skip empty line
           && line.charAt(0) != '%' // skip comments
           )
    ) {
      {
        val newSection = line match {

          // line is section declarator, modify section
          case starRegex(id) => {
            line match {
              case starRegex(expr) => {
                val newSection = expr.toLowerCase
                // check that new section is valid
                if (newSection != "vertices"
                  && newSection != "arcs" && newSection != "arcslist"
                  && newSection != "edges" && newSection != "edgeslist"
                )
                  throw new Exception("Pajek file format only accepts"
                    + " Vertices, Arcs, Edges, Arcslist, Edgeslist"
                    + " as section declarator: line " + lineNumber)
                // check there is no more than one vertices section
                if (newSection == "vertices") {
                  if (nodeNumber != -1)
                    throw new Exception(
                      "There must be one and only one vertices section"
                    )
                  // read nodeNumber
                  nodeNumber = line match {
                    case verticesRegex(expr) => expr.toLong
                    case _ => throw new Exception(
                      s"Cannot read node number: line $lineNumber"
                    )
                  }
                }
                section = "section_def"
                newSection
              }
            }
          }
          // line is not section declarator,
          // section does not change
          case _ => section
        }

        /** *************************************************************************
          * Read edge information
          * **************************************************************************/
        if (section == "edges" || section == "arcs") {
          val newEdge = line match {
            case edgeRegex1(src, dst) =>
              // check that index is in valid range
              if (1 <= src.toLong && src.toLong <= nodeNumber
                && 1 <= dst.toLong && dst.toLong <= nodeNumber)
                ((src.toLong, dst.toLong), 1.0)
              else throw new Exception(
                s"Vertex index must be within [1,$nodeNumber]: line $lineNumber"
              )
            case edgeRegex2(src, dst, weight) =>
              // check that index is in valid range
              if (1 <= src.toLong && src.toLong <= nodeNumber
                && 1 <= dst.toLong && dst.toLong <= nodeNumber) {
                // check that weight is not negative
                if (weight.toDouble < 0) throw new Exception(
                  s"Edge weight must be non-negative: line $lineNumber"
                )
                ((src.toLong, dst.toLong), weight.toDouble)
              }
              else throw new Exception(
                s"Vertex index must be within [1,$nodeNumber]: line $lineNumber"
              )
            // check vertex parsing is correct
            case _ => throw new Exception(
              s"Edge definition error: line $lineNumber"
            )
          }
          edges += newEdge
        }

        /***************************************************************************
          * prepare for next loop
          ***************************************************************************/
        section = newSection
        lineNumber += 1
      }
    }

    edges.toSeq
  }

  def run_pagerank(edges: Seq[((VertexId, VertexId), Double)], numIter: Int = 20, resetProb: Double = 0.15, norm: Boolean = true): Map[VertexId, Double] = {
    val weightSum: Map[VertexId, Double] = edges.map {
      case ((from, to), weight) => (from, weight)
    }.reduceByKey()

    val normEdge = edges.map {
      case ((from, to), weight) => ((from, to), weight / weightSum(from))
    }

    val initRank = (edges.map { case ((from, to), weight) => (from, resetProb) } ++
      edges.map { case ((from, to), weight) => (to, resetProb) }).toSet
    val nodeRank = scala.collection.mutable.Map(initRank.toSeq: _*)

    var iteration = 0
    while (iteration < numIter) {
      val sum = normEdge.map {
        case ((from, to), weight) => (to, weight * nodeRank(from))
      }.reduceByKey()

      sum.foreach {
        case (to, weight) => nodeRank(to) = resetProb + (1.0 - resetProb) * weight
      }

      iteration += 1
    }

    if (norm) {
      //norm
      val rank_sum = nodeRank.map { case (_, size) => size }.sum
      nodeRank.keys.foreach(k => nodeRank(k) = nodeRank(k) / rank_sum)
    }

    nodeRank.toMap
  }


  implicit class ArrayReduce[K: ClassTag, V: ClassTag](collection: Traversable[Tuple2[K, V]]) {
    implicit def reduceByKey()(implicit num: Numeric[V]) = {
      import num._
      collection
        .groupBy(_._1)
        .map { case (group: K, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }
    }

  }

}
