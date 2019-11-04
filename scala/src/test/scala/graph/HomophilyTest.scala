package graph

import graph.GraphApplication.trainData

object HomophilyTest {

  def calcHomophily(data: String, minDegree: Option[Int], maxDegree: Option[Int], columns: String*) = {
    val graph = if (maxDegree.isDefined) {
      GraphApplication.buildAndFilterGraph(trainData, true, minDegree.get, maxDegree.get, columns: _*)
    }
    else {
      GraphApplication.buildGraph(trainData, true, columns: _*)
    }

    println(f"$columns :")
    GraphHelper.homophily(graph)
    println()
  }

  def run() = {
    val trainData = GraphApplication.trainData
    val columns = Seq(
      "zhuce_phone", "phone1", "phone2",
      "company_name", "company_tel", "company_address",
      "imei", "imsi", "idfa", "ip")
    for (column <- columns) {
      calcHomophily(trainData, None, None, column)
    }

    calcHomophily(trainData, Some(1), Some(500), "mac")
//    calcHomophily(trainData, Some(1), Some(500), "phone1", "phone2")
  }

  def main(args: Array[String]): Unit = {
    run()
  }

}
