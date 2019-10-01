package graph

import org.apache.spark.graphx._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object BiPageRank  {

  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }
  var log_ = LoggerFactory.getLogger(logName)

  def run[ED: ClassTag] (graph: Graph[Node, ED], numIter: Int, resetProb: Double = 0.15
                                                ): Graph[Double, Double] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")


    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute resetProb.
    // When running personalized pagerank, only the source vertex
    // has an attribute resetProb. All others are set to 0.
    var rankGraph: Graph[(Node, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => (vdata, deg.getOrElse(0)) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr._2, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) => (attr._1, attr._1.score * resetProb)
    }

    var iteration = 0
    var prevRankGraph: Graph[(Node, Double), Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2 * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph

      //init score as restart score
      val rPrb = (node:Node) => node.score * resetProb

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => {
          val nRank = rPrb(oldRank._1) + (1.0 - resetProb) * msgSum
          (oldRank._1, nRank)
        }
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
//      log_.info(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

//      rankGraph.vertices.foreach(println)
      iteration += 1
    }

    rankGraph.mapVertices( (id, attr) => (attr._2 ) )
  }

}
