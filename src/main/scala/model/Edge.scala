package model

case class Edge(
  v1: Int
, v2: Int
) {
  def reversed: Edge = Edge(v2, v1)
}
