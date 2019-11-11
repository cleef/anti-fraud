package graph

import graph.GraphApplication.{propScoreSaveDir, trainData}

object ScorePropTest {

  /**
    * -Xmx2048m
    *
    *  F:/data/prop_score/part-00000-xxxx.csv -> F:/data/prop_fscore.csv
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val app:GraphApplication = new GraphApplication()
    app.propFraudScore(trainData, propScoreSaveDir)
  }

}
