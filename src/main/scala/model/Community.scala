package model

case class Community(
  id: Int
, vertices: Seq[Int]
, in_degree: Int
, out_degrees: Seq[(Int, Int)] // community id -> degree
, total_out: Int
, total_deg: Int
, density: Double
, modularity: Double
)
