import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


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
  val edges: RDD[(Int, Int)] = spark.read
    .textFile(filePath)
    .rdd
    .map { s =>
      val a = s.split(" ").map(_.toInt)
      (a(0), a(1))
    }
  edges.cache()

  val adjLists: RDD[(Int, Iterable[Int])] = edges
    .flatMap { case (v1, v2) => Seq( (v1, v2), (v2, v1) ) } // add reversed edges
    .groupByKey()
  adjLists.cache()

  val e: Double = edges.count()
  val v: Double = adjLists.count()
  val graphTotalDegree: Double = 2 * e

  logger.warn(s"Total degree: $graphTotalDegree, V = $v, E = $e")

  val (resCommunities, (modularity, regularization)): (RDD[(Int, Seq[Int])], (Double, Double)) = getCommunities
  logger.warn(s"Metrics: Modularity = $modularity, Regularization = $regularization, Q = ${modularity + regularization}")

  resCommunities
    .mapValues(_.mkString(", "))
    .values
    .saveAsTextFile("communities")

  spark.close()



  def commDensity(vertCount: Double, inDegree: Double): Double =
    if (vertCount < 1) 0.0
    else if (vertCount == 1) 1.0
    else inDegree / (vertCount * (vertCount - 1.0))

  def commModularity(commTotalDegree: Double, inDegree: Double): Double =
    inDegree / graphTotalDegree - Math.pow(commTotalDegree / graphTotalDegree, 2.0)


  def getCommunities: (RDD[(Int, Seq[Int])], (Double, Double)) = {
    // Assign each vertex to own community
    val initialCommunities: RDD[(Int, Seq[Int])] = adjLists.map { case (id, _) => (id, Seq(id)) }
    initialCommunities.cache()

    val (initVertCommStats, initCommStats, (initTotalDensity, initModularity, initRegularization)) = getSplitStats(initialCommunities)
    getCommunitiesStep(initialCommunities, initVertCommStats, initCommStats, initTotalDensity, initModularity, initRegularization, 0)
  }



  def getSplitStats(communities: RDD[(Int, Seq[Int])]): (RDD[(Int, (Int, Int, Seq[(Int, Int)]))], RDD[(Int, (Int, Int, Int, Double))], (Double, Double, Double)) = {

    val vertToComm: RDD[(Int, Int)] = communities
      .flatMap( c => c._2.map( (_, c._1) ) )
//    logger.warn(s"vertToComm size: ${vertToComm.count}")

    // vertId -> (degree, vertCommId, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val vertCommStats: RDD[(Int, (Int, Int, Seq[(Int, Int)]))] = edges
      .keyBy(_._1)
      .join(vertToComm)
      .map { case (v1, (e, v1Comm)) => (e._2, (v1, v1Comm)) }
      .join(vertToComm)
      .flatMap { case (v2, ((v1, v1Comm), v2Comm)) => Seq( ((v1, v1Comm), v2Comm), ((v2, v2Comm), v1Comm) ) }
      .groupByKey()
      .map { case ((v1, v1Comm), adjComms) =>
        ( v1
        , ( adjComms.size
          , v1Comm
          , adjComms.groupBy(v => v).mapValues(_.size).toSeq
          )
        )
      }
    vertCommStats.cache()
//    logger.warn(s"commAdjListsByVert size: ${commAdjListsByVert.count}")

    // commId -> (vertCount, inDegree, outDegree)
    val commStats: RDD[(Int, (Int, Int, Int, Double, Double))] = vertCommStats
      .map { case (_, (_, commId, adjCommList)) => (commId, (1, adjCommList)) } // 1 to count vertices per community
      .reduceByKey { case ((vertCount1, commMap1), (vertCount2, commMap2)) =>
        ( vertCount1 + vertCount2
        , (commMap1 ++ commMap2).groupBy( _._1 ).mapValues( _.map(_._2).sum ).toSeq )
      }
      .map { case (commId, (vertCount, adjCommList)) =>
        val inDegree = adjCommList.filter(_._1 == commId).map(_._2).sum
        val outDegree = adjCommList.filter(_._1 == commId).map(_._2).sum

        ( commId
        , ( vertCount
          , inDegree
          , outDegree
          , commModularity(inDegree + outDegree, inDegree)
          , commDensity(vertCount, inDegree)
          )
        )
      }
    commStats.cache()
//    logger.warn(s"commStats size: ${commAdjLists.count}")

    val (modularity, totalDensity) = commStats
      .map ( s => (s._2._4, s._2._5) )
      .reduce( (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) )

    val n = commStats.count()

    (vertCommStats, commStats.mapValues( s => (s._1, s._2, s._3, s._5)), (totalDensity, modularity, 0.5*(totalDensity/n - n.toDouble/v)))
  }


  @tailrec
  def getCommunitiesStep( prevCommunities: RDD[(Int, Seq[Int])]
                        , prevVertCommStats: RDD[(Int, (Int, Int, Seq[(Int, Int)]))]
                        , prevCommStats: RDD[(Int, (Int, Int, Int, Double))]
                        , prevTotalDensity: Double
                        , prevModularity: Double
                        , prevRegularization: Double
                        , step: Int ): (RDD[(Int, Seq[Int])], (Double, Double)) = {

    logger.warn(s"Step #$step")

    val prevCommCount = prevCommStats.count()

    // get all possible transitions
    val goodMoves: RDD[(Int, Int, Int)] = prevVertCommStats
      .flatMap{ case (vertId, (_, commId, adjCommList)) =>
        adjCommList
          .filter(_._1 != commId)
          .map { case (adjCommId, _) => (vertId, (commId, adjCommId)) }
      }
      .keyBy(_._2._1)
      .join( prevCommStats )
      .map{ case (vertCommId, ((vertId, (_, adjCommId)), vertCommStats)) =>
        (vertId, ((vertCommId, vertCommStats), adjCommId))
      }
      .keyBy(_._2._2)
      .join( prevCommStats )
      .map { case (adjCommId, ((vertId, ((vertCommId, vertCommStats), _)), adjCommStats)) =>
        (vertId, ((vertCommId, vertCommStats), (adjCommId, adjCommStats)))
      }
      .keyBy(_._1)
      .join( prevVertCommStats )
      .map { case (vertId, ((_, ((vertCommId, vertCommStats), (adjCommId, adjCommStats))), vertexCommStats)) =>
        (vertId, vertexCommStats._1, vertexCommStats._3, (vertCommId, vertCommStats), (adjCommId, adjCommStats))
      }
      .flatMap {
        case(vertId, vertDegree, adjCommList, (vertCommId, vertCommStats), (adjCommId, adjCommStats)) =>
          val adjCommMap = adjCommList.toMap

          val (vertCommVertCount, vertCommInDegree, vertCommOutDegree, vertCommDensity) = vertCommStats
          val (adjCommVertCount, adjCommInDegree, adjCommOutDegree, adjCommDensity) = adjCommStats

          val deltaModularity =
            (adjCommMap.getOrElse(adjCommId, 0) - adjCommMap.getOrElse(vertCommId, 0)) / e +
            vertDegree * ((vertCommInDegree + vertCommOutDegree) - (adjCommInDegree + adjCommOutDegree) - vertDegree) / (2.0 * e * e)

          val prevRegularization = 0.5 * (prevTotalDensity/prevCommCount - prevCommCount/v)
          val deltaDensity = commDensity(vertCommVertCount - 1, vertCommInDegree - 2 * adjCommMap.getOrElse(vertCommId, 0)) -
                             vertCommDensity +
                             commDensity(adjCommVertCount + 1, adjCommInDegree + 2 * adjCommMap.getOrElse(adjCommId, 0)) -
                             adjCommDensity
          val currCommCount = if (vertCommVertCount > 1) prevCommCount else prevCommCount - 1
          val currRegularization = 0.5 * ((prevTotalDensity + deltaDensity)/currCommCount - currCommCount/v)

          val deltaRegularization = currRegularization - prevRegularization

          val deltaQ = deltaModularity + deltaRegularization

          if (deltaQ > 0.000001) List( ((vertId, vertCommId, adjCommId), deltaQ) )
          else Nil
      }
    .keyBy(_._1._1)
    .reduceByKey { case ((upd1, deltaQ1), (upd2, deltaQ2)) => if (deltaQ1 > deltaQ2) (upd1, deltaQ1) else (upd2, deltaQ2) }
    .values
    .sortBy(-_._2)
    .map(_._1)
//    logger.warn(s"goodMoves size: ${goodMoves.count}")

    val update = sc.parallelize(
      goodMoves
        .aggregate((Seq[(Int, Int, Int)](), Set[Int]()))(
          seqOp = {
            case ( (moves, usedComms), (vertId, commFrom, commTo) ) =>
              if (usedComms.contains(commFrom) || usedComms.contains(commTo)) (moves, usedComms)
              else ( moves ++ Seq((vertId, commFrom, commTo)), usedComms ++ Set(commFrom, commTo))
          }
          , combOp = {
              case ( (moves1, usedComms1), (moves2, _) ) =>
                val movesNew = moves2
                  .filter{ case (_, commFrom, commTo) => !usedComms1.contains(commFrom) && !usedComms1.contains(commTo) }
                val newUsedComms = movesNew.map(_._2).union(movesNew.map(_._3)).distinct.filter(c => !usedComms1.contains(c))
                (moves1 ++ movesNew, usedComms1 ++ newUsedComms)
            }
          )
        ._1
      )
      .map( u => (u._1, u._3) )


//    logger.warn(s"update size: ${update.count}")



    if (update.isEmpty) (prevCommunities, (prevModularity, prevRegularization))
    else {
      val newCommunities = prevVertCommStats
        .map( v => (v._1, v._2._2) ) // vertId, commId
        .leftOuterJoin(update)
        .map { case (vertId, (currCommId, update)) => (update.getOrElse(currCommId), vertId) }
        .groupByKey()
        .map { case (_, vs) => (vs.min, vs.toSeq) }
      newCommunities.cache()
      logger.warn(s"Communities count: ${newCommunities.count}")

//      println(s"New communities")
//      newCommunities
//        .collect()
//        .sortBy(_._1)
//        .foreach(println)
//      println()

      val (newVertCommStats, newCommStats, (newTotalDensity, newModularity, newRegularization)) = getSplitStats(newCommunities)

      val prevQ = prevModularity + prevRegularization
      val newQ = newModularity + newRegularization

//      println()
//      goodMoves.collect().sortBy(-_._2).foreach(println)
//      println()
//      update.foreach(println)
//      println()
//      newCommunities
//        .collect()
//        .sortBy(_._1)
//        .foreach(v => println((v._1, v._2.sorted)))
      logger.warn(s"Modularity: $prevModularity -> $newModularity")
      logger.warn(s"Regularization: $prevRegularization -> $newRegularization")
      logger.warn(s"Q: $prevQ -> $newQ")

      if (newQ < prevQ + 0.000001) {
        if (prevQ < newQ) (newCommunities, (newModularity, newRegularization))
        else (prevCommunities, (prevModularity, prevRegularization))
      }
      else {
        prevCommunities.unpersist(false)
        prevVertCommStats.unpersist(false)
        prevCommStats.unpersist(false)
        getCommunitiesStep(newCommunities, newVertCommStats, newCommStats, newTotalDensity, newModularity, newRegularization, step+1)
      }
    }
  }

}

