package graph

import graph.GraphLoader

object LoaderTest  {

  def main(args: Array[String]): Unit = {
    val trainData = GraphApplication.trainData
    val df = GraphLoader.loadData(trainData)
  }

}
