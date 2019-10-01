package graph

import graph.GraphApplication.{propScoreData, testData, userScoreSaveDir}

object ScoreUserTest {

  def main(args: Array[String]): Unit = {
    val app:GraphApplication = new GraphApplication()

    app.userFraudScore(testData, propScoreData, userScoreSaveDir)
  }


}
