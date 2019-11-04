package graph

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD
import redsea.LocalSparkContext

import scala.reflect.ClassTag

object GraphHelper extends LocalSparkContext {
  import spark.implicits._

  def goodBadRate[ED: ClassTag](g:Graph[Node, ED])  = {
    val bad:VertexRDD[Int] = g.aggregateMessages[Int](
      ctx => if(ctx.srcAttr.flag == Node.flag_bad) ctx.sendToDst(1),
      (a, b) => (a+b)
    )

    val good:VertexRDD[Int] = g.aggregateMessages[Int](
      ctx => if(ctx.srcAttr.flag == Node.flag_good) ctx.sendToDst(1),
      (a, b) => (a+b)
    )

    val goodBad = good.leftJoin(bad)((vertexId, a, b) => (a, b))
    val goodBadDf = goodBad.map( r => (r._1, r._2._1, r._2._2.getOrElse(0)) ).toDF("node_id", "good_cnt", "bad_cnt")

    goodBadDf.withColumn("bad_rate", goodBadDf.col("bad_cnt")/goodBadDf.col("good_cnt") )
  }

  def run_pagerank[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED],numIter:Int = 5) = {
//    val ranks = PageRank.runUntilConvergence(subgraph, 0.0001).vertices
    val ranks = PageRank.run(graph, 5).vertices
    ranks
  }

  def degree[VD: ClassTag, ED: ClassTag](g:Graph[VD, ED])= {
    g.outDegrees.toDF("vertex", "outDegree").describe("outDegree").show
    g.inDegrees.toDF("vertex", "inDegree").describe("inDegree").show

    println("vertex count:" + g.vertices.count)
    println("edge count:" + g.edges.count)
  }

  def joinInDegree[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]) = {
    val degrees: VertexRDD[Int] = graph.inDegrees
    val g = graph.outerJoinVertices(degrees) {(vid, data, deg) => (data, deg.getOrElse(0))}

    g
  }

  def joinOutDegree[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED]) = {
    val degrees: VertexRDD[Int] = graph.outDegrees
    val g = graph.outerJoinVertices(degrees) {(vid, data, deg) => (data, deg.getOrElse(0))}

    g
  }

  def subGraphByInDegree[VD: ClassTag, ED: ClassTag](graph:Graph[VD, ED], minInDegree:Int = 5, maxInDegree:Int = 5):Graph[(VD, Int), ED] = {
    val cutGraph = joinInDegree(graph).subgraph(
      vpred = (vid: VertexId, deg:(VD, Int)) => deg._2 == 0 || ( deg._2 >= minInDegree && deg._2 <= maxInDegree )
    )

    //intDegree!=0 or outDegree !=0
    val isolatedFilterGraph = joinOutDegree(cutGraph).subgraph (
      vpred =  (vid: VertexId, deg: ((VD, Int), Int)) => deg._2 != 0 || deg._1._2 != 0
    )

    val result = isolatedFilterGraph.mapVertices( (vid: VertexId, deg: ((VD, Int), Int)) => deg._1 )
    result
  }

  /**
    *  TODO
    *
    * @param graph
    * @tparam ED
    */
  def projection[ED: ClassTag](graph:Graph[Node, ED], minWeight:Int = 1) = {
    // (prop_id1, (uid1, uid2, uid3))
    val nbrIds: VertexRDD[Array[VertexId]] =  graph.collectNeighborIds(EdgeDirection.In)

    // (uid1, (uid1, uid2, uid3))
    // (uid2, (uid1, uid2, uid3))
    // (uid3, (uid1, uid2, uid3))
    val flat_step1: RDD[(VertexId, Array[VertexId])] = nbrIds.flatMap(e =>  for (id <- e._2) yield  (id, e._2) )

    // (uid1, uid2) (uid1, uid3)
    // (uid2, uid3)
    val flat_step2: RDD[(VertexId, VertexId)] = flat_step1.flatMap(e => for (id <- e._2 if (e._1 < id)) yield  (e._1, id) )

    // create graph
    val edges: RDD[Edge[Int]] = flat_step2.map( e => (e, 1) ).reduceByKey(_+_).filter( _._2 >= minWeight).map(e => Edge(e._1._1, e._1._2, e._2))
    val nodes: VertexRDD[Node] = graph.vertices.filter(e => e._2.label.equals("user"))
    val g = Graph(nodes, edges)
    g
  }

  def connectedness[ED: ClassTag](graph:Graph[Node, ED]) =  {
    val vertexNum = graph.vertices.count()
    val edge = graph.edges.count()

    1.0 * edge * 2 / ( vertexNum * (vertexNum - 1) )
  }

  def dyadicity[ED: ClassTag](graph:Graph[Node, ED], p: Double) = {
    val badNodeCnt = graph.vertices.filter( n => n._2.flag != Node.flag_good ).count

    val expectedNumber = p * badNodeCnt * (badNodeCnt - 1) / 2
    val edgeNumber = graph.triplets.filter(
      t => ( t.srcAttr.flag != Node.flag_good && t.dstAttr.flag != Node.flag_good)
    ).count()

    1.0 * edgeNumber  / expectedNumber
  }

  def heterophilicity[ED: ClassTag](graph:Graph[Node, ED], p: Double) = {
    val badNodeCnt = graph.vertices.filter( n => n._2.flag != Node.flag_good ).count
    val goodNodeCnt = graph.vertices.count - badNodeCnt

    val expectedNumber = p * badNodeCnt * goodNodeCnt
    val crossEdgeNumber = graph.triplets.filter( t =>
      ( t.srcAttr.flag == Node.flag_good && t.dstAttr.flag != Node.flag_good) ||
        (t.srcAttr.flag != Node.flag_good && t.dstAttr.flag == Node.flag_good)
    ).count()

    1.0 * crossEdgeNumber  / expectedNumber
  }

  def homophily(graph:Graph[Node, Int]) = {
    val userGraph = GraphHelper.projection(graph)
    val connected = GraphHelper.connectedness(userGraph)
    val dyadicity = GraphHelper.dyadicity(userGraph, connected)
    val heterophilicity = GraphHelper.heterophilicity(userGraph, connected)
    println(f"connectedness=$connected, dyadicity=$dyadicity, heterophilicity=$heterophilicity")
  }


  /**
    * (0,38711)
    * (1,10338)
    * (3,48659)
    *
    */
  def flagCount(graph:Graph[Node, Int]) = {
    val nodes: VertexRDD[Node] = graph.vertices.filter(e => e._2.label.equals("user") )

    val flags = nodes.map( e => (e._2.flag, 1) )
    val count = flags.reduceByKey( _ + _ )
    count.foreach(println)
  }

  def nodeCount(graph:Graph[Node, Int]) = {
    val count = graph.vertices.map( e => (e._2.label, 1) ).reduceByKey(_ + _)
    count.foreach(println)
  }

  /**
    */
  def hubNodes(graph:Graph[Node, Int]) = {
    val degree = joinInDegree(graph).vertices
    degree.sortBy(e => -e._2._2).take(20).foreach(println)
  }


}
