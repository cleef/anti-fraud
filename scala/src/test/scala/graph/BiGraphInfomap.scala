package graph

import redsea.{LocalSpark, LocalSparkContext}
import community.{InfoGraph, Infomap, ModuleId}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}

class BiGraphInfomap  extends FunSuite with BeforeAndAfter with LocalSparkContext {
  import spark.implicits._

  test("bipartite graph") {
    val graph = load_graph()
    val edges: Array[((VertexId, VertexId), Double)] = graph.edges.map(e => ((e.srcId, e.dstId), 1.0) ).collect()

    //run partition
    val infograph = InfoGraph(edges)
    val partition = Infomap.run(infograph, 100)

    //filter group
    val group: RDD[(VertexId, ModuleId)] = GraphApplication.sc.parallelize(partition.moduleIdx.toSeq)
    val groupIds = filter_group(graph, group)

    //filter graph
    val nodeGraph = graph.filter[ModuleId, Int](
      preprocess =  g => g.outerJoinVertices(group) { case (_, _, mid) => mid.getOrElse(-1) },
      vpred = (vid, mid) => groupIds.contains(mid)
    )

    //draw graph
    val fileName =   "F:/anti-fraud/data/output/group_graph.png"
    Visualization.plot(nodeGraph, fileName)
  }

  /**
    *
    * (50298,(10,4))
    * (-474504547,(10,5))
    * (35270,(11,4))
    * (-1090571725,(16,4))
    * (72596,(12,5))
    * (92608,(10,5))
    * (609945438,(12,3))
    * (2094808212,(10,5))
    * (-1072444798,(33,3))
    * (61399,(13,4))
    * (-1299800906,(12,4))
    * (-2081414418,(11,4))
    * (-878933364,(14,4))
    *
    */
  test("show graph") {
    var targetGroupIds = Seq(-1072444798, -1090571725, -474504547, 2094808212, 2094808212, -878933364)

    val graph = load_graph()
    val group: RDD[(VertexId, VertexId)] = LocalSpark.loadCsv(spark, "F:/anti-fraud/data/output/group.csv").map{ r => (r.getAs[String]("vid").toLong ,r.getAs[String]("gid").toLong) }.rdd

//    targetGroupId =
    for(gid <- targetGroupIds)
      plot_single_group(graph, group, gid)
  }

  def plot_single_group(graph:Graph[Node, Int], group: RDD[(VertexId, VertexId)] , targetGroupId:ModuleId) = {

    //filter graph
    val nodeGraph = graph.filter[ModuleId, Int](
      preprocess =  g => g.outerJoinVertices(group) { case (_, _, mid) => mid.getOrElse(-1) },
      vpred = (vid, mid) => mid  == targetGroupId
    )

    println(nodeGraph.vertices.count)

    //draw graph
    val fileName =   "F:/anti-fraud/data/output/group_graph_" + targetGroupId + ".png"
    Visualization.plot(nodeGraph, fileName)
  }

  def load_graph(projection:Boolean = false) = {
    val graph = GraphApplication.buildAndFilterGraph(GraphApplication.trainData, true, 2, 50, GraphApplication.attrColumns:_* )
    if(projection)
      GraphHelper.projection(graph, 2)
    else
      graph
  }

  def filter_group(g:Graph[graph.Node, Int], group: RDD[(VertexId, ModuleId)], save:Boolean = true) = {
    val min_group = 10
    val max_group = 100
    //filter group by member size
    val groupCnt = group.map( { case(vid, cid) => (cid, 1) } ).reduceByKey(_+_).filter({
      case (cid, cnt) => cnt >=min_group & cnt <=max_group
    })

    //filter group by member types
    val groupNodeTypeCnt = group.join(g.vertices).map{
      case(vid, (mid, node)) => (mid, node.label)}.distinct().map{
      case(mid, label) => (mid, 1)}.reduceByKey(_+_).filter{
      case(mid, cnt) => cnt > 2
    }

    //group to display
    val displayGroup = groupCnt.join(groupNodeTypeCnt)
    val groupIds = Set(displayGroup.map( _._1 ).collect():_*)

    if(save)
    {
      val filterGroup = group.filter { case(vid, mid) => groupIds.contains(mid) }
      LocalSpark.saveAsCsv(filterGroup.toDF("vid", "gid"), "F:/anti-fraud/data/output/group.csv")
    }

    displayGroup.collect.sortBy(_._1).foreach(println)
    groupIds
  }
}
