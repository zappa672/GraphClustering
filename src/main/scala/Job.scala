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

  val filePath: String = this.getClass.getResource("/input/test0.graph").getPath

  logger.warn(s"Read graph data $filePath.")
  val (edges, adjLists) = loadGraph(filePath)

  val e: Double = edges.count()
  val v: Double = adjLists.count()
  val graphTotalDegree: Double = 2 * e

  logger.warn(s"Total degree: $graphTotalDegree, V = $v, E = $e")

  val communities: RDD[(Int, Seq[Int])] = getCommunities

  val (modularity, regularization) = calcMetrics(communities, true)
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
      .map { case (_, (vertCommId, adjCommList)) => (vertCommId, (1, adjCommList)) } // 1 to count vertices per community
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

    val prevDensity = commStats
      .map( c => commDensity(c._2._1, c._2._2) )
      .sum

    val prevCommCount = commStats.count.toDouble

    // get all possible transitions
    val possibleMoves = commAdjListsByVert
      .flatMap { case (vertId, (vertCommId, adjCommList)) =>
        adjCommList
          .map { case(adjCommId, _) => (vertId, adjCommList, vertCommId, adjCommId) }
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

          val prevRegularization = 0.5 * (prevDensity/prevCommCount - prevCommCount/v)
          val deltaDensity = commDensity(vertCommVertCount - 1, vertCommInDegree - 2 * adjCommMap.getOrElse(vertCommId, 0)) -
                             commDensity(vertCommVertCount, vertCommInDegree) +
                             commDensity(adjCommVertCount + 1, adjCommInDegree + 2 * adjCommMap.getOrElse(adjCommId, 0)) -
                             commDensity(adjCommVertCount, adjCommInDegree)
          val currCommCount = if (vertCommVertCount > 1) prevCommCount else prevCommCount - 1
          val currRegularization = 0.5 * ((prevDensity + deltaDensity)/currCommCount - currCommCount/v)

          val deltaRegularization = currRegularization - prevRegularization

          val deltaQ = deltaModularity + deltaRegularization

          if (deltaQ > 0.000001) List((CommunityUpdate(vertId, vertCommId, adjCommId), deltaQ, deltaModularity, deltaRegularization))
          else Nil
      }

    val update = sc.parallelize( goodMoves
      .map( m => (m._1, m._2))
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


      val (prevMod, prevReg) = calcMetrics(prevCommunities, true)
      val (newMod, newReg) = calcMetrics(newCommunities, false)

      val prevQ = prevMod + prevReg
      val newQ = newMod + newReg


      logger.warn(s"Step #$step")
//      println()
//      goodMoves.collect().sortBy(-_._2).foreach(println)
//      println()
//      update.foreach(println)
//      println()
//      newCommunities
//        .collect()
//        .sortBy(_._1)
//        .foreach(v => println((v._1, v._2.sorted)))
      logger.warn(s"Modularity: $prevMod -> $newMod")
      logger.warn(s"Regularization: $prevReg -> $newReg")
      logger.warn(s"Q: $prevQ -> $newQ")

      if (newQ < prevQ + 0.000001) {
        if (prevQ < newQ) newCommunities
        else prevCommunities
      }
      else getCommunitiesStep(newCommunities, step + 1)
    }
  }


  def calcMetrics(communities: RDD[(Int, Seq[Int])], debug: Boolean): (Double, Double) = {
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
        ( vertCount1 + vertCount2, (commMap1 ++ commMap2).groupBy( _._1 ).mapValues( _.map(_._2).sum ).toSeq
        )
      }

    val modAndDensByComm = commAdjLists
      .map { case (commId, (vertCount, adjCommList)) =>
        val adjCommListSplit = adjCommList.partition(_._1 == commId)
        val inDegree = adjCommListSplit._1.map(_._2).sum
        val outDegree = adjCommListSplit._2.map(_._2).sum
        ( commModularity(inDegree + outDegree, inDegree), commDensity(vertCount, inDegree) )
      }

//    if (debug) {
//      println
//      modAndDensByComm.foreach(println)
//      println
//    }

    val (modularity, totalDensity) = modAndDensByComm
      .reduce( (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) )

    val n = communities.count()

    (modularity, 0.5*(totalDensity/n - n.toDouble/v))
  }

}
