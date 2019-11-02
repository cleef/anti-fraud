package graph

case class Node(label:String,  value:String, flag:Int, score:Double)

object Node {
  val flag_good = 0
  val flag_bad = 1
  val flag_reject = 3

}



