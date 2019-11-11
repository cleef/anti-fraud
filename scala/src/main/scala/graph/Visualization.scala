package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.stream.file.FileSinkImages
import org.graphstream.stream.file.FileSinkImages.{LayoutPolicy, OutputType, Quality, Resolutions}

object Visualization {

  val stylesheet = "file:///F:/anti-fraud/data/stylesheet"

  def flag2str(flag:Int) = {
    flag match {
      case 1 => "bad30"
      case 0 => "good"
      case 3 => "reject"
      case 2 => "bad7"
      case _ => "good"
    }
  }

  def plot(g:Graph[Node, Int], fileName:String) = {
    val graph: SingleGraph = new SingleGraph("graphDemo")
    graph.addAttribute("ui.stylesheet", s"url('$stylesheet')")

    for ((id, attr) <- g.vertices.collect()){
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]

      var uiclass = attr.label
      if(attr.label.equals("user"))
          uiclass = uiclass + "_" + flag2str(attr.flag)
      node.setAttribute("ui.class", uiclass)
      node.setAttribute("ui.label", attr.value + ":" + attr.label)
    }

    for (Edge(x, y, attr) <- g.edges.collect().distinct){
      val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }

    val pic = new FileSinkImages(OutputType.png, Resolutions.HD1080)
    pic.setLayoutPolicy(LayoutPolicy.COMPUTED_FULLY_AT_NEW_IMAGE)
    pic.setQuality(Quality.HIGH)
    pic.writeAll(graph, fileName)
  }

}
