
import org.apache.spark.graphx.VertexId
import scala.collection.mutable.HashSet

package object graph {

  type VertexSet = HashSet[VertexId]

}
