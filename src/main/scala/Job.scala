import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Job extends App {

  sealed class StepType()
  case object MoveVertices extends StepType
  case object JoinCommunities extends StepType


  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName(s"GraphClustering")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val filePath: String = this.getClass.getResource("/input/test0.graph").getPath

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
    .flatMap { case (v1, v2) => Seq((v1, v2), (v2, v1)) } // add reversed edges
    .groupByKey()
  adjLists.cache()

  val e: Double = edges.count()
  val v: Double = adjLists.count()
  val graphTotalDegree: Double = 2 * e

//  def main(args: Array[String]): Unit = {

    logger.warn(s"Total degree: $graphTotalDegree, V = $v, E = $e")

    val (resModularity, resRegularization, resCommunities): (Double, Double, RDD[(Int, Seq[Int])]) = getCommunities
    logger.warn(s"Metrics: Modularity = $resModularity, Regularization = $resRegularization, Q = ${resModularity + resRegularization}")

//    resCommunities
//      .mapValues(_.mkString(", "))
//      .values
//      .saveAsTextFile("communities")

    spark.close()

//  }

  def commDensity(vertCount: Double, inDegree: Double): Double =
    if (vertCount < 1) 0.0
    else if (vertCount == 1) 1.0
    else inDegree / (vertCount * (vertCount - 1.0))

  def commModularity(commTotalDegree: Double, inDegree: Double): Double =
    inDegree / graphTotalDegree - Math.pow(commTotalDegree / graphTotalDegree, 2.0)


  def getCommunities: (Double, Double, RDD[(Int, Seq[Int])]) = {
    // Assign each vertex to own community
    val initialCommunities: RDD[(Int, Seq[Int])] = adjLists.map { case (id, _) => (id, Seq(id)) }
    initialCommunities.cache()

    println("\nInitial communities:")
    initialCommunities.foreach(println)
    println()

    val ((initCommCount, initModularity, initTotalDensity, initRegularization), initCommAdjLists, initCommStats, initVertToCommBc) = getSplitStats(initialCommunities)
    getCommunitiesStep( initialCommunities
                      , initCommCount
                      , initModularity
                      , initTotalDensity
                      , initRegularization
                      , initCommAdjLists
                      , initCommStats
                      , initVertToCommBc
                      , 1 )
  }

  def getSplitStats(communities: RDD[(Int, Seq[Int])])
    : ( (Long, Double, Double, Double)
      , RDD[(Int, (Int, Seq[(Int, Int)]))]
      , RDD[(Int, (Int, Int, Int, Double, Double))]
      , Broadcast[Seq[Int]]) = {

    val vertToComm: RDD[(Int, Int)] = communities
      .flatMap( c => c._2.map( (_, c._1) ) )
    //    logger.warn(s"vertToComm size: ${vertToComm.count}")

    val vertToCommBc: Broadcast[Seq[Int]] = sc.broadcast( vertToComm.collect().sortBy(_._1).map(_._2).toSeq )

    // vertId -> (degree, vertCommId, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val vertCommStats: RDD[(Int, (Int, Int, Seq[(Int, Int)]))] = adjLists
      .map { case (vertId, nbrIds) =>
        val vertToCommSeq = vertToCommBc.value
        val vertCommId = vertToCommSeq(vertId)

        ( vertId
        , ( nbrIds.size
          , vertCommId
          , nbrIds
            .map( adjVertId => vertToCommSeq(adjVertId) )
            .groupBy( v => v )
            .mapValues(_.size)
            .toSeq
          )
        )
      }
    //    logger.warn(s"commAdjListsByVert size: ${commAdjListsByVert.count}")

    // commId -> (vertexCount, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val commAdjLists: RDD[(Int, (Int, Seq[(Int, Int)]))] = vertCommStats
      .map { case (_, (_, commId, adjCommList)) => (commId, (1, adjCommList)) } // 1 to count vertices per community
      .reduceByKey { case ((vertCount1, commMap1), (vertCount2, commMap2)) =>
        ( vertCount1 + vertCount2
        , (commMap1 ++ commMap2).groupBy( _._1 ).mapValues( _.map(_._2).sum ).toSeq )
      }
    commAdjLists.cache()

    // commId -> (vertCount, inDegree, outDegree)
    val commStats: RDD[(Int, (Int, Int, Int, Double, Double))] = commAdjLists
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

    val commCount = commStats.count()
    val regularization = 0.5*(totalDensity/commCount - commCount/v)

    ((commCount, modularity, totalDensity, regularization), commAdjLists, commStats, vertToCommBc)

  }

  def getCommunitiesStep( prevCommunities: RDD[(Int, Seq[Int])]
                        , prevCommCount: Double
                        , prevModularity: Double
                        , prevTotalDensity: Double
                        , prevRegularization: Double
                        , prevCommAdjLists: RDD[(Int, (Int, Seq[(Int, Int)]))]
                        , prevCommStats: RDD[(Int, (Int, Int, Int, Double, Double))]
                        , prevVertToCommBc: Broadcast[Seq[Int]]
                        , step: Int)
  : ( Double, Double, RDD[(Int, Seq[Int])] ) = {

    logger.warn(s"Step #$step")

    val goodJoins = prevCommAdjLists
      .mapValues(_._2)
      .flatMap { case (commId, adjCommList) =>
        adjCommList
          .filter { case (adjCommId, _) => adjCommId > commId }
          .map { case (adjCommId, adjCommDegree) => (commId, (adjCommId, adjCommDegree)) }
      }
      .join(prevCommStats)
      .map { case (commId1, ((commId2, interDegree), commStats1)) =>
        (commId2, ((commId1, commStats1), interDegree))
      }
      .join(prevCommStats)
      .flatMap {
        case (commId2, (((commId1, commStats1), interDegree), commStats2)) =>
          val (vertCount1, inDegree1, outDegree1, modularity1, density1) = commStats1
          val (vertCount2, inDegree2, outDegree2, modularity2, density2) = commStats2

          val joinedVertCount = vertCount1 + vertCount2
          val joinedInDegree = inDegree1 + inDegree2 + 2 * interDegree
          val joinedOutDegree = outDegree1 + outDegree2 - 2 * interDegree

          val joinedModularity = commModularity(joinedInDegree + joinedOutDegree, joinedInDegree)
          val joinedDensity = commDensity(joinedVertCount, joinedInDegree)

          val deltaModularity = joinedModularity - (modularity1 + modularity2)
          val deltaDensity = joinedDensity - (density1 + density2)

          val currRegularization = 0.5 * ((prevTotalDensity + deltaDensity)/(prevCommCount - 1.0) - (prevCommCount - 1.0)/v)

          val deltaRegularization = currRegularization - prevRegularization

          val deltaQ = deltaModularity + deltaRegularization

          if (deltaQ > 0.000001) List( ((commId1, commId2), deltaQ) )
          else Nil
      }

    val update = sc.parallelize(
      goodJoins
        .sortBy(-_._2)
        .map { case ((commId1, commId2), deltaQ) => (commId1, commId2) }
        .aggregate((Seq[(Int, Int)](), Set[Int]()))(
          seqOp = {
            case ( (joines, usedComms), (commId1, commId2) ) =>
              if (usedComms.contains(commId1) || usedComms.contains(commId2)) (joines, usedComms)
              else ( joines ++ Seq((commId1, commId2)), usedComms ++ Set(commId1, commId2))
          }
          , combOp = {
            case ( (moves1, usedComms1), (moves2, _) ) =>
              val movesNew = moves2
                .filter{ case (commId1, commId2) => !usedComms1.contains(commId1) && !usedComms1.contains(commId2) }
              val newUsedComms = movesNew.map(_._1).union(movesNew.map(_._2)).distinct.filter(c => !usedComms1.contains(c))
              (moves1 ++ movesNew, usedComms1 ++ newUsedComms)
          }
        )
        ._1
    )

    val commsToUpdate = update.map(_._1).union( update.map(_._2) ).map( (_, 1) )
    val notUsedCommunities = prevCommunities
      .leftOuterJoin(commsToUpdate)
      .flatMap { case (commId, (commVertices, hasUpdate)) => if (hasUpdate.isDefined) Seq[(Int, Seq[Int])]() else Seq( (commId, commVertices) ) }

    val joinedCommunities = update
      .join(prevCommunities)
      .map { case (commId1, (commId2, commVertices1)) => (commId2, (commId1, commVertices1)) }
      .join(prevCommunities)
      .map { case (commId2, ((commId1, commVertices1), commVertices2)) =>
        val joinedVertices = commVertices1 ++ commVertices2
        (joinedVertices.min, joinedVertices)
      }

   val newCommunities = notUsedCommunities.union(joinedCommunities)

    newCommunities.cache()
    logger.warn(s"Communities count: ${newCommunities.count}")

    //      println(s"New communities")
    //      newCommunities
    //        .collect()
    //        .sortBy(_._1)
    //        .foreach(println)
    //      println()


    val ((newCommCount, newModularity, newTotalDensity, newRegularization), newCommAdjLists, newCommStats, newVertToCommBc) = getSplitStats(newCommunities)

    val prevQ = prevModularity + prevRegularization
    val newQ = newModularity + newRegularization

    logger.warn(s"Modularity: $prevModularity -> $newModularity")
    logger.warn(s"Regularization: $prevRegularization -> $newRegularization")
    logger.warn(s"Q: $prevQ -> $newQ")

    prevCommunities.unpersist(false)
    prevCommStats.unpersist(false)
    prevVertToCommBc.destroy()

    if (newQ < prevQ + 0.000001) {
      if (prevQ < newQ) (newModularity, newRegularization, newCommunities)
      else (prevModularity, prevRegularization, prevCommunities)
    }
    else {
      getCommunitiesStep( newCommunities
        , newCommCount
        , newModularity
        , newTotalDensity
        , newRegularization
        , newCommAdjLists
        , newCommStats
        , newVertToCommBc
        , step + 1 )
    }

  }



/*
  def getSplitStats(communities: RDD[(Int, Seq[Int])]): ( RDD[(Int, (Int, Int, Seq[(Int, Int)]))]
                                                        , RDD[(Int, (Int, Int, Int, Double))]
                                                        , (Double, Double, Double)
                                                        , Broadcast[Seq[Int]] ) = {

    val vertToComm: RDD[(Int, Int)] = communities
      .flatMap( c => c._2.map( (_, c._1) ) )
//    logger.warn(s"vertToComm size: ${vertToComm.count}")

    val vertToCommBc: Broadcast[Seq[Int]] = sc.broadcast( vertToComm.collect().sortBy(_._1).map(_._2).toSeq )

    // vertId -> (degree, vertCommId, (commId1 -> commId1degree, commId2 -> commId2degree, ...))
    val vertCommStats: RDD[(Int, (Int, Int, Seq[(Int, Int)]))] = adjLists
      .map { case (vertId, nbrIds) =>
        val vertToCommSeq = vertToCommBc.value
        val vertCommId = vertToCommSeq(vertId)

        ( vertId
        , ( nbrIds.size
          , vertCommId
          , nbrIds
              .map( adjVertId => vertToCommSeq(adjVertId) )
              .groupBy( v => v )
              .mapValues(_.size)
              .toSeq
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

    (vertCommStats, commStats.mapValues( s => (s._1, s._2, s._3, s._5)), (totalDensity, modularity, 0.5*(totalDensity/n - n.toDouble/v)), vertToCommBc)
  }



  @tailrec
  def getCommunitiesStep( prevCommunities: RDD[(Int, Seq[Int])]
                        , prevVertCommStats: RDD[(Int, (Int, Int, Seq[(Int, Int)]))]
                        , prevCommStats: RDD[(Int, (Int, Int, Int, Double))]
                        , prevTotalDensity: Double
                        , prevModularity: Double
                        , prevRegularization: Double
                        , vertToCommBc: Broadcast[Seq[Int]]
                        , stepType: StepType
                        , step: Int ): (RDD[(Int, Seq[Int])], (Double, Double)) = {

    logger.warn(s"Step #$step")

    val prevCommCount = prevCommStats.count()

    val update = stepType match {
      case MoveVertices =>
        // get all possible transitions
        val goodMoves: RDD[(Int, Int, Int)] = prevVertCommStats
          .flatMap{ case (vertId, (vertDegree, currCommId, adjCommList)) =>
            adjCommList
              .filter(_._1 != currCommId)
              .map { case (adjCommId, _) =>
                val vertCurrCommStats = adjCommList.filter(_._1 == currCommId)
                val vertAdjCommStats = adjCommList.filter(_._1 == adjCommId)

                val vertCurrCommDegree = if (vertCurrCommStats.isEmpty) 0 else vertCurrCommStats.head._2
                val vertAdjCommDegree = if (vertAdjCommStats.isEmpty) 0 else vertAdjCommStats.head._2

                (vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree))
              }
          }
          .keyBy(_._2._2)
          .join( prevCommStats )
          .map{ case (_, ((vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree)), currCommStats)) =>
            (vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree, currCommStats))
          }
          .keyBy(_._2._3)
          .join( prevCommStats )
          .map { case (_, ((vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree, currCommStats)), adjCommStats)) =>
            (vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree, currCommStats, adjCommStats))
          }
          .flatMap {
            case(vertId, (vertDegree, currCommId, adjCommId, vertCurrCommDegree, vertAdjCommDegree, currCommStats, adjCommStats)) =>
              val (currCommVertCount, currCommInDegree, currCommOutDegree, currCommDensity) = currCommStats
              val (adjCommVertCount, adjCommInDegree, adjCommOutDegree, adjCommDensity) = adjCommStats

              val deltaModularity = (vertAdjCommDegree - vertCurrCommDegree) / e +
                vertDegree * ((currCommInDegree + currCommOutDegree) - (adjCommInDegree + adjCommOutDegree) - vertDegree) / (2.0 * e * e)

              val deltaDensity = commDensity(currCommVertCount - 1, currCommInDegree - 2 * vertCurrCommDegree) - currCommDensity +
                commDensity(adjCommVertCount + 1, adjCommInDegree + 2 * vertAdjCommDegree) - adjCommDensity
              val currCommCount = if (currCommVertCount > 1) prevCommCount else prevCommCount - 1
              val currRegularization = 0.5 * ((prevTotalDensity + deltaDensity)/currCommCount - currCommCount/v)

              val deltaRegularization = currRegularization - prevRegularization

              val deltaQ = deltaModularity + deltaRegularization

              if (deltaQ > 0.000001) List( ((vertId, currCommId, adjCommId), deltaQ) )
              else Nil
          }
          .keyBy(_._1._1)
          .reduceByKey { case ((upd1, deltaQ1), (upd2, deltaQ2)) => if (deltaQ1 > deltaQ2) (upd1, deltaQ1) else (upd2, deltaQ2) }
          .values
          .sortBy(-_._2)
          .map(_._1)
        //    logger.warn(s"goodMoves size: ${goodMoves.count}")

        sc.parallelize(
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
      case JoinCommunities =>

//        val possibleJoins = prevCommStats
//          .flatMap { case (commId, (_, _, _, _, _, adjCommList)) =>
//            adjCommList.flatMap { case (adjCommId, _) => if (commId < adjCommId) Seq( (commId, adjCommId) ) else Seq[(Int, Int)]() }
//          }
//
//        possibleJoins
//          .join( prevCommStats )
//          .map{ case (commId1, (commId2, сommStats1)) =>
//            (commId2, (commId1, сommStats1))
//          }
//          .join( prevCommStats )
//          .map { case (commId2, ((commId1, сommStats1), сommStats2)) =>
//            ((commId1, сommStats1), (commId2, сommStats2))
//          }
//          .flatMap { case ((commId1, сommStats1), (commId2, сommStats2)) =>
//
//          }

        sc.parallelize( Seq[(Int, Int)]() )
    }



//    logger.warn(s"update size: ${update.count}")
    if (update.isEmpty) (prevCommunities, (prevModularity, prevRegularization))
    else {
      val newCommunities = stepType match {
        case MoveVertices =>
          prevVertCommStats
            .map( v => (v._1, v._2._2) ) // vertId, commId
            .leftOuterJoin(update)
            .map { case (vertId, (currCommId, update)) => (update.getOrElse(currCommId), vertId) }
            .groupByKey()
            .map { case (_, vs) => (vs.min, vs.toSeq) }
        case JoinCommunities =>
          prevCommunities
      }
      newCommunities.cache()
      logger.warn(s"Communities count: ${newCommunities.count}")

//      println(s"New communities")
//      newCommunities
//        .collect()
//        .sortBy(_._1)
//        .foreach(println)
//      println()

      val (newVertCommStats, newCommStats, (newTotalDensity, newModularity, newRegularization), newVertToCommBc) = getSplitStats(newCommunities)

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

      prevCommunities.unpersist(false)
      prevVertCommStats.unpersist(false)
      prevCommStats.unpersist(false)
      vertToCommBc.destroy()

      if (newQ < prevQ + 0.000001) {
        if (prevQ < newQ) (newCommunities, (newModularity, newRegularization))
        else (prevCommunities, (prevModularity, prevRegularization))
      }
      else {
        getCommunitiesStep(newCommunities, newVertCommStats, newCommStats, newTotalDensity, newModularity, newRegularization, newVertToCommBc, MoveVertices, step+1)
      }
    }
  }

 */

}
