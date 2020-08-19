package model

case class VertexCommData(
  id: Int
, degree: Int
, commId: Int
, adjComm: Seq[(Int, Int)] // commId -> degree
)
