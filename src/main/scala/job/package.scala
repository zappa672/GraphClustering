import job.Job.spark
import model.{Edge, VertexData}
import org.apache.spark.rdd.RDD

package object job {

  def loadGraph(path: String): RDD[VertexData] = spark.read
    .textFile(path)
    .rdd
    .map(s => s.split(" ").map(_.toInt))
    .flatMap(a => Seq(Edge(a(0), a(1)), Edge(a(1), a(0)))) // add reversed edges
    .keyBy(_.v1)
    .mapValues(_.v2)
    .groupByKey(12)
    .map(g => VertexData(id = g._1, adj = g._2.toSeq))

  def commDensity(vertCount: Int, inDegree: Int): Double =
    if (vertCount < 1) 0.0
    else if (vertCount == 1) 1.0
    else inDegree.toDouble / (vertCount * (vertCount - 1.0))

  def commModularity(graphTotalDegree: Int, commTotalDegree: Int, inDegree: Int): Double =
    inDegree.toDouble / graphTotalDegree - Math.pow(commTotalDegree.toDouble / graphTotalDegree, 2.0)

}
