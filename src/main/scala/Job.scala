import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import model._


object Job extends App {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName(s"GraphClustering")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val filePath: String = this.getClass.getResource("/input/1.graph").getPath

  logger.warn(s"Read graph data $filePath.")
  val (edges, adjLists) = loadGraph(filePath)

  val e: Double = edges.count()
  val v: Double = adjLists.count()
  val graphTotalDegree: Double = 2 * e

  logger.warn(s"Total degree: $graphTotalDegree, V = $v, E = $e")

  val communities: RDD[(Int, Seq[Int])] = getCommunities
  //    logger.warn( s"Communities: \n" + communities.collect().sortBy(_.min).map(_.toList.sorted).mkString("\n") + "\n" )

  val (modularity, regularization) = calcMetrics(communities)
  logger.warn(s"Metrics: Modularity = $modularity, Regularization = $regularization, Q = ${modularity + regularization}")

  spark.close()


  def loadGraph(path: String): (RDD[Edge], RDD[VertexData]) = {
    val edges = spark.read
      .textFile(path)
      .rdd
      .map{ s =>
        val a = s.split(" ").map(_.toInt)
        Edge(a(0), a(1))
      }
    edges.cache()

    val adjLists = edges
      .flatMap(e => Seq(e, e.reversed)) // add reversed edges
      .keyBy(_.v1)
      .mapValues(_.v2)
      .groupByKey()
      .map(g => VertexData(id = g._1, adj = g._2.toSeq))
    adjLists.cache()

    (edges, adjLists)
  }


  def commDensity(vertCount: Double, inDegree: Double): Double =
    if (vertCount < 1) 0.0
    else if (vertCount == 1) 1.0
    else inDegree / (vertCount * (vertCount - 1.0))

  def commModularity(commTotalDegree: Double, inDegree: Double): Double =
    inDegree / graphTotalDegree - Math.pow(commTotalDegree / graphTotalDegree, 2.0)


  def getCommunities: RDD[(Int, Seq[Int])] = {
    val initialComms = adjLists.map( v => (v.id, Seq(v.id)) )

//    println(s"Initial communities")
//    initialComms
//      .collect()
//      .sortBy(_._1)
//      .foreach(println)
//    println()

    val finalCommunities = getCommunitiesStep(initialComms, 0)

//    println(s"Final communities")
//    finalCommunities
//      .collect()
//      .sortBy(_._1)
//      .foreach(println)
//    println()

    finalCommunities
  }

  @tailrec
  def getCommunitiesStep(prevCommunities: RDD[(Int, Seq[Int])], step: Int): RDD[(Int, Seq[Int])] = {
    val vertToComm = prevCommunities
      .flatMap( c => c._2.map( (_, c._1) ) )

    val vertToCommBc = sc.broadcast( vertToComm.collect() )

    // vertId -> (vertCommId, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val commAdjListsByVert: RDD[(Int, (Int, Seq[(Int, Int)]))] = adjLists
      .map { l =>
        val vertToCommMap = vertToCommBc.value.toMap
        ( l.id
        , ( vertToCommMap(l.id)
        , l.adj.map( vertToCommMap(_) ).groupBy( v => v ).mapValues( _.size ).toSeq ) )
      }

    // commId -> (vertCount, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val commAdjLists: RDD[(Int, (Int,  Seq[(Int, Int)]))] = commAdjListsByVert
      .map { case (vertId, (vertCommId, adjCommList)) => (vertCommId, (1, adjCommList)) } // 1 to count vertices per community
      .reduceByKey { case ((vertCount1, commMap1), (vertCount2, commMap2)) =>
        ( vertCount1 + vertCount2
        , (commMap1 ++ commMap2).groupBy( _._1 ).mapValues( _.map(_._2).sum ).toSeq )
      }

    // commId -> (vertCount, inDegree, outDegree)
    val commStats = commAdjLists
        .map { case (commId, (vertCount, adjCommList)) =>
          ( commId
          , (vertCount
          , adjCommList.filter(_._1 == commId).map(_._2).sum
          , adjCommList.filter(_._1 != commId).map(_._2).sum ) )
        }

    // get all possible transitions
    val possibleMoves = commAdjListsByVert
      .flatMap { case (vertId, (vertCommId, adjCommList)) =>
        adjCommList
          .map { case(adjCommId, degree) => (vertId, adjCommList, vertCommId, adjCommId) }
          .filter( v => v._3 != v._4 )
      }
      .keyBy(_._3)
      .join(commStats)
      .map { case (_, ((vertId, adjCommList, vertCommId, adjCommId), vertCommStats)) =>
        (vertId, adjCommList, (vertCommId, vertCommStats), adjCommId)
      }
      .keyBy(_._4)
      .join(commStats)
      .map { case (_, ((vertId, adjCommList, vertCommStats, adjCommId), adjCommStats)) =>
        (vertId, adjCommList, vertCommStats, (adjCommId, adjCommStats))
      }

    // get all possible transitions
    val goodMoves = possibleMoves
      .flatMap {
        case(vertId, adjCommList, (vertCommId, vertCommStats), (adjCommId, adjCommStats)) =>
          val adjCommMap = adjCommList.toMap
          val (vertCommVertCount, vertCommInDegree, vertCommOutDegree) = vertCommStats
          val (adjCommVertCount, adjCommInDegree, adjCommOutDegree) = adjCommStats
          val vertDegree = adjCommList.map(_._2).sum

          val deltaModularity =
            (adjCommMap.getOrElse(adjCommId, 0) - adjCommMap.getOrElse(vertCommId, 0)) / e +
              vertDegree * ((vertCommInDegree + vertCommOutDegree) - (adjCommInDegree + adjCommOutDegree) - vertDegree) / (2.0 * e * e)
          val deltaRegularity = 0.0
          val deltaQ = deltaModularity + deltaRegularity

          if (deltaQ > 0.000001) List((CommunityUpdate(vertId, vertCommId, adjCommId), deltaQ))
          else Nil
      }

    val update = sc.parallelize( goodMoves
      .keyBy(_._1.vertexId)
      .groupByKey()
      .mapValues( v => v.maxBy(_._2) )
      .values
      .collect()
      .sortBy(-_._2)
      .foldLeft((Seq[CommunityUpdate](), Set[Int]())) {
        case (acc, (upd, _)) =>
          if (acc._2.contains(upd.commFrom) || acc._2.contains(upd.commTo)) acc
          else (acc._1 ++ Seq(upd), acc._2 ++ Set(upd.commFrom, upd.commTo))
      }
      ._1
    )

//    println("Update")
//    update.collect.foreach(println)
//    println()

    if (update.isEmpty) prevCommunities
    else {
      val newVertToComm = vertToComm
        .leftOuterJoin(update.keyBy(_.vertexId))
        .map { case (vertId, (currCommId, update)) => (update.fold(currCommId)(_.commTo), vertId) }
      val newCommunities = newVertToComm
        .groupByKey()
        .map { case (_, vs) => (vs.min, vs.toSeq) }

//      println(s"New communities")
//      newCommunities
//        .collect()
//        .sortBy(_._1)
//        .foreach(println)
//      println()

      val (prevMod, prevReg) = calcMetrics(prevCommunities)
      val (newMod, newReg) = calcMetrics(newCommunities)

      val prevQ = prevMod + prevReg
      val newQ = newMod + newReg

      println(s"Step #$step, Q: $prevQ -> $newQ")

      if (newQ < prevQ + 0.000001) {
        if (prevQ < newQ) newCommunities
        else prevCommunities
      }
      else getCommunitiesStep(newCommunities, step + 1)
    }
  }


  def calcMetrics(communities: RDD[(Int, Seq[Int])]): (Double, Double) = {
    val vertToCommBc = sc.broadcast( communities
      .flatMap( c => c._2.map( (_, c._1) ) )
      .collect() )

    // vertCommId -> (1, (commId1 -> commId1degree, commId2 -> commId2degree, ...)), 1 to count vertices per community
    val commAdjListsByVert: RDD[(Int, (Int, Seq[(Int, Int)]))] = adjLists
      .map { l =>
        val vertToCommMap = vertToCommBc.value.toMap
        ( vertToCommMap(l.id)
        , ( 1
          , l.adj.map( vertToCommMap(_) ).groupBy( v => v ).mapValues( _.size ).toSeq
          )
        )
      }

    val commAdjLists: RDD[(Int, (Int,  Seq[(Int, Int)]))] = commAdjListsByVert
      .reduceByKey { case ((vertCount1, commMap1), (vertCount2, commMap2)) =>
        ( vertCount1 + vertCount2
        , (commMap1 ++ commMap2).groupBy( _._1 ).mapValues( _.map(_._2).sum ).toSeq
        )
      }

    val modAndDensByComm = commAdjLists
      .map { case (commId, (vertCount, adjCommList)) =>
        val adjCommListSplitted = adjCommList.partition(_._1 == commId)
        val inDegree = adjCommListSplitted._1.map(_._2).sum
        val outDegree = adjCommListSplitted._2.map(_._2).sum
        ( commModularity(inDegree + outDegree, inDegree), commDensity(vertCount, inDegree) )
      }

    val (modularity, totalDensity) = modAndDensByComm
      .reduce( (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) )

    val n = communities.count()

    (modularity, 0.5*(totalDensity/n - n.toDouble/v))
  }


  /*
  def getCommunities: RDD[Community] = {
    // Initialize communities assigning each vertex to it's own community
    val initialComms = adjLists
      .map(v =>
        Community(
          id = v.id
          , vertices = Seq(v.id)
          , in_degree = 0
          , out_degrees = v.adj.map(_ -> 1) // per community
          , total_out = v.adj.size
          , total_deg = v.adj.size
          , density = 1.0
          , modularity = commModularity(v.adj.size, 0)
        )
      )

    println("Initial communities")
    initialComms
      .collect()
      .sortBy(_.id)
      .foreach(println)
    println("")

    val finalCommunities = getCommunitiesStep(initialComms, 0)

    println("Final communities")
    finalCommunities
      .collect()
      .sortBy(_.id)
      .foreach(println)
    println("")

    finalCommunities
  }

  @tailrec
  def getCommunitiesStep(communities: RDD[Community], step: Int): RDD[Community] = {
    val totalDens = communities.map(_.density).sum()
    val commCount = communities.count()

    val vertexCommStats = getVertexCommStats(communities)

    val availableMoves = vertexCommStats
      .flatMap { v => v.adjComm.map(a => (v.id, v.degree, v.adjComm, v.commId, a._1)) }

    val goodMoves = availableMoves
      .keyBy(_._4)
      .join(communities.keyBy(_.id))
      .map { case (_, ((vertId, vertDegree, vertAdjComm, commFromId, commToId), commFrom)) =>
        (vertId, vertDegree, vertAdjComm, commFrom, commToId)
      }
      .keyBy(_._5)
      .join(communities.keyBy(_.id))
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

          if (deltaQ > 0.000001) List((CommunityUpdate(vertId, commFrom.id, commTo.id), deltaQ))
          else Nil
        }

        if (goodMoves1.nonEmpty) List(goodMoves1.maxBy(_._2))
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

    val newCommunities = updateCommunities(communities, sc.parallelize(update), vertexCommStats)

    val prevMetrics = calcMetrics(communities)
    val currMetrics = calcMetrics(newCommunities)

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
    else getCommunitiesStep(newCommunities, step + 1)
  }


  // update:  pairs vertex, community id
  def updateCommunities(communities: RDD[Community], update: RDD[CommunityUpdate], vertCommStats: RDD[VertexCommData]): RDD[Community] = {
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
          .map(v => (v._2._1, v._2._2.get))
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
                , modularity = commModularity(new_in_degree + new_out_degrees.map(_._2).sum, new_in_degree)
              ))
          }
      )
  }


  def calcMetrics(communities: RDD[Community]): (Double, Double) = {
    val (total_modularity, total_density) = communities
      .map(c => (c.modularity, c.density))
      .reduce { case (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) }

    val n = communities.count().toDouble

    (total_modularity, 0.5 * (total_density / n - n / v))
  }


  def getVertexCommStats(communities: RDD[Community]): RDD[VertexCommData] = {
    val vertToComm = communities
      .flatMap(c => c.vertices.map(v => (v, c.id)))

    adjLists
      .flatMap(v => v.adj.map((v.id, _)))
      .keyBy(_._2)
      .join(vertToComm) // (vertId2, ((vertId1, vertId2), commId2))
      .map(v => (v._2._1._1, v._2._2)) // vertId1, commId2
      .groupByKey()
      .mapValues(adjComms => adjComms.groupBy(v => v).mapValues(_.size).toSeq)
      .join(vertToComm) // (vertId1, (Iterable[commId2, count], commId1)
      .map(v =>
        VertexCommData(
          id = v._1
          , degree = v._2._1.map(_._2).sum
          , commId = v._2._2
          , adjComm = v._2._1
        )
      )
  }
  */
}
