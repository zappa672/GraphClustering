package model

case class VertexData(
  id: Int
, adj: Seq[Int]
) {
  val degree  = adj.size
}
