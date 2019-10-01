package graph

import graph.GraphApplication.{propScoreSaveDir, trainData}

object ScorePropTest {

  def main(args: Array[String]): Unit = {
    val app:GraphApplication = new GraphApplication()
    app.propFraudScore(trainData, propScoreSaveDir)
  }

}
