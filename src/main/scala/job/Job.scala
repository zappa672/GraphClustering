package job

import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import model._


object Job {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName(s"GraphClustering")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {

    val filePath = this.getClass.getResource("/input/test0.graph").getPath
    //    val commFilePath = this.getClass.getResource("/input/2.communities").getPath

    logger.warn(s"Read graph data $filePath.")
    val adjLists = loadGraph(filePath)

    val (v, totalDegree) = adjLists
      .map(v => (1, v.adj.size))
      .reduce { case (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) }
    adjLists.cache()
    logger.warn( s"Total degree: $totalDegree, V = $v" )

    val communities = getCommunities(totalDegree, v, adjLists)
    //    logger.warn( s"Communities: \n" + communities.collect().sortBy(_.min).map(_.toList.sorted).mkString("\n") + "\n" )

    val (modularity, regularization) = calcMetrics(v, communities)
    logger.warn(s"Metrics: Modularity = $modularity, Regularization = $regularization, Q = ${modularity + regularization}")

    spark.close()

  }




  def getCommunities(totalDegree: Int, v: Int, adjLists: RDD[VertexData]): RDD[Community] = {
    val initialComms = initCommunities(totalDegree, adjLists)

    println("Initial communities")
    initialComms
      .collect()
      .sortBy(_.id)
      .foreach(println)
    println("")

    val finalCommunities = getCommunitiesStep(totalDegree / 2, v, adjLists, getVertexCommStats(adjLists, initialComms), initialComms, 0)

    println("Final communities")
    finalCommunities
      .collect()
      .sortBy(_.id)
      .foreach(println)
    println("")

    finalCommunities
  }

  @tailrec
  def getCommunitiesStep(e: Int, v: Int, adjLists: RDD[VertexData], vertexCommStats: RDD[VertexCommData], communities: RDD[Community], step: Int): RDD[Community] = {
    val totalDens = communities.map(_.density).sum()
    val commCount = communities.count()

    val availableMoves = vertexCommStats
      .flatMap { v => v.adjComm.map( a => (v.id, v.degree, v.adjComm, v.commId, a._1) ) }

    val goodMoves = availableMoves
      .keyBy(_._4)
      .join( communities.keyBy(_.id) )
      .map { case (_, ((vertId, vertDegree, vertAdjComm, commFromId, commToId), commFrom)) =>
        (vertId, vertDegree, vertAdjComm, commFrom, commToId)
      }
      .keyBy(_._5)
      .join( communities.keyBy(_.id) )
      .map { case (_, ((vertId, vertDegree, vertAdjComm, commFrom, commToId), commTo)) =>
        (vertId, (vertDegree, vertAdjComm, commFrom, commTo))
      }
      .groupByKey()
      .flatMap { case (vertId, moves) =>
        val goodMoves1 = moves.flatMap { case (vertDegree, vertAdjComm, commFrom, commTo) =>
          val vertDegCommFrom = vertAdjComm.toMap.getOrElse(commFrom.id, 0)
          val vertDegCommTo = vertAdjComm.toMap.getOrElse(commTo.id, 0)

          val deltaModularity = (vertDegCommTo - vertDegCommFrom) / e.toDouble +
            vertDegree * (commFrom.total_deg - commTo.total_deg - vertDegree) / (2.0 * e * e)

          val deltaRegularization = 0.0

          val deltaQ = deltaModularity + deltaRegularization

          if (deltaQ > 0.000001) List( (CommunityUpdate(vertId, commFrom.id, commTo.id), deltaQ) )
          else Nil
        }

        if (goodMoves1.nonEmpty) List( goodMoves1.maxBy(_._2) )
        else Nil
      }

    println("Good moves")
    goodMoves
      .collect()
      .sortBy(-_._2)
      .foreach(println)
    println("")

    val update = goodMoves
      .collect()
      .sortBy(-_._2)
      .foldLeft((Seq[CommunityUpdate](), Set[Int]())) {
        case (acc, (upd, _)) =>
          if (acc._2.contains(upd.commFrom) || acc._2.contains(upd.commTo)) acc
          else (acc._1 ++ Seq(upd), acc._2 ++ Set(upd.commFrom, upd.commTo))
      }
      ._1

    println("Independent good moves")
    update
      .foreach(println)
    println("")

    val newCommunities = updateCommunities(2 * e, communities, sc.parallelize(update), vertexCommStats)

    val prevMetrics = calcMetrics(v, communities)
    val currMetrics = calcMetrics(v, newCommunities)

    println("New communities")
    newCommunities
      .collect()
      .sortBy(_.id)
      .foreach(println)
    println("")

    println(s"Curr metrics: $currMetrics, Prev metrics: $prevMetrics\n")

    val prevQ = prevMetrics._1 + prevMetrics._2
    val currQ = currMetrics._1 + currMetrics._2

    if (currQ < prevQ + 0.000001) {
      if (prevQ < currQ) newCommunities
      else communities
    }
    else getCommunitiesStep(e, v, adjLists, getVertexCommStats(adjLists, newCommunities), newCommunities, step+1)
  }


  // Initialize communities assigning each vertex to it's own community
  def initCommunities(totalDegree: Int, adjLists: RDD[VertexData]): RDD[Community] =
    adjLists
      .map(v =>
        Community(
          id = v.id
          , vertices = Seq(v.id)
          , in_degree = 0
          , out_degrees = v.adj.map(_ -> 1) // per community
          , total_out = v.adj.size
          , total_deg = v.adj.size
          , density = 1.0
          , modularity = commModularity(totalDegree, v.adj.size, 0)
        )
      )

  // update:  pairs vertex, community id
  def updateCommunities(totalDegree: Int, communities: RDD[Community], update: RDD[CommunityUpdate], vertCommStats: RDD[VertexCommData]): RDD[Community] = {
    val commsWithUpdates = communities
      .keyBy(_.id)
      .leftOuterJoin(update.flatMap(u => Seq((u.commFrom, (u.vertexId, false)), (u.commTo, (u.vertexId, true)))))

//    println("commsWithUpdates")
//    commsWithUpdates
//      .filter(_._2._2.isDefined)
//      .map( v => (v._2._1, v._2._2.get) )
//      .keyBy(_._2._1)
//      .join(vertCommStats.keyBy(_.id))
//      .foreach(println)
//    println("")

    commsWithUpdates
      .filter(_._2._2.isEmpty)
      .map(_._2._1)
      .union(
        commsWithUpdates
          .filter(_._2._2.isDefined)
          .map( v => (v._2._1, v._2._2.get) )
          .keyBy(_._2._1)
          .join(vertCommStats.keyBy(_.id))
          .flatMap { j =>
            val comm = j._2._1._1
            val update = j._2._1._2

            val (vertId, append) = update
            val vertCommStats = j._2._2
            val vertAdjComms = vertCommStats.adjComm.toMap

            val (new_vertices, new_in_degree, new_out_degrees) = {
              if (append) { // add vertex to community
                (comm.vertices ++ Iterable(vertId)
                  , comm.in_degree + 2 * vertAdjComms.getOrElse(comm.id, 0)
                  , comm.out_degrees
                      .flatMap { case (commId, deg) =>
                        if (commId != vertCommStats.commId) Seq((commId, deg))
                        else {
                          val new_deg = deg - vertAdjComms.getOrElse(comm.id, 0)
                          if (new_deg > 0) Seq((commId, new_deg))
                          else Seq[(Int, Int)]()
                        }
                      }
                      .map(v => if (!vertAdjComms.contains(v._1)) v else (v._1, v._2 + vertAdjComms(v._1))) ++
                    vertAdjComms.filter(_._1 != comm.id).filter(a => !comm.out_degrees.toMap.contains(a._1))
                )
              } else { // remove vertex from community
                (comm.vertices.filter(_ != vertId)
                  , comm.in_degree - 2 * vertAdjComms.getOrElse(comm.id, 0)
                  , comm.out_degrees
                  .map(v => if (!vertAdjComms.contains(v._1)) v else (v._1, v._2 - vertAdjComms(v._1)))
                  .filter(_._2 > 0)
                )
              }
            }

            if (new_vertices.isEmpty) Seq[Community]()
            else
              Seq(Community(
                id = new_vertices.min
                , vertices = new_vertices
                , in_degree = new_in_degree
                , out_degrees = new_out_degrees
                , total_out = new_out_degrees.map(_._2).sum
                , total_deg = new_in_degree + new_out_degrees.map(_._2).sum
                , density = commDensity(new_vertices.size, new_in_degree)
                , modularity = commModularity(totalDegree, new_in_degree + new_out_degrees.map(_._2).sum, new_in_degree)
              ))
          }
      )
  }


  def calcMetrics(v: Long, communities: RDD[Community]): (Double, Double) = {
    val (total_modularity, total_density) = communities
      .map(c => (c.modularity, c.density))
      .reduce { case (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) }

    val n = communities.count().toDouble

    (total_modularity, 0.5 * (total_density / n - n / v))
  }


  def getVertexCommStats(adjLists: RDD[VertexData], communities: RDD[Community]): RDD[VertexCommData] = {
    val vertToComm = communities
      .flatMap( c => c.vertices.map( v => (v, c.id) ) )

    adjLists
      .flatMap( v => v.adj.map( (v.id, _) ) )
      .keyBy(_._2)
      .join(vertToComm) // (vertId2, ((vertId1, vertId2), commId2))
      .map( v => (v._2._1._1, v._2._2) ) // vertId1, commId2
      .groupByKey()
      .mapValues( adjComms => adjComms.groupBy(v => v).mapValues(_.size).toSeq )
      .join(vertToComm) // (vertId1, (Iterable[commId2, count], commId1)
      .map( v =>
        VertexCommData (
          id = v._1
        , degree = v._2._1.map(_._2).sum
        , commId = v._2._2
        , adjComm = v._2._1
        )
      )
  }

}
