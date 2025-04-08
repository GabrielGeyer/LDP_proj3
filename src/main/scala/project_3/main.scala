package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.util.Random

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    // Step 1: Initialize all vertices with label 0 (undecided)
    var g = g_in.mapVertices((id, _) => 0)

    var remaining = g.vertices.filter { case (_, attr) => attr == 0 }.count()
    var iteration = 0

    while (remaining > 0) {
      iteration += 1
      println(s"Iteration $iteration — Remaining active vertices: $remaining")

      // Step 2: Assign random priorities to active vertices (0 = undecided)
      val priorities = g.vertices.mapValues((attr: Int) =>
      if (attr == 0) Random.nextDouble() else Double.PositiveInfinity
      )

      val gWithPriorities = Graph(priorities, g.edges)

      // Step 3: Send neighbor priorities to each vertex
      val neighborMin = gWithPriorities.aggregateMessages[Double](
        triplet => {
          if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr != Double.PositiveInfinity) {
            triplet.sendToSrc(triplet.dstAttr)
            triplet.sendToDst(triplet.srcAttr)
          }
        },
        (a, b) => math.min(a, b)
      )

      // Step 4: Determine which vertices win — lowest among neighbors
      val newLabels = gWithPriorities.vertices.leftJoin(neighborMin) {
        case (_, selfPriority, Some(minNeighbor)) =>
          if (selfPriority < minNeighbor) 1 else 0
        case (_, selfPriority, None) => 1 // No neighbors → MIS by default
      }

      // Update graph with MIS candidates marked as 1
      val updated = g.vertices.leftJoin(newLabels) {
        case (_, oldLabel, Some(newLabel)) =>
          if (oldLabel != 0) oldLabel else newLabel
        case (_, oldLabel, None) => oldLabel
      }

      g = Graph(updated, g.edges)

      // Step 5: Remove neighbors of MIS vertices (mark them -1)
      val toRemove = g.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0)
            triplet.sendToDst(-1)
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0)
            triplet.sendToSrc(-1)
        },
        (a, b) => -1
      )

      val finalVerts = g.vertices.leftJoin(toRemove) {
        case (_, attr, Some(-1)) =>
          if (attr == 0) -1 else attr
        case (_, attr, _) => attr
      }

      g = Graph(finalVerts, g.edges)

      remaining = g.vertices.filter { case (_, attr) => attr == 0 }.count()
      println(s" Iteration $iteration complete — Remaining: $remaining")
    }

    println("LubyMIS complete")
    g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {

    // Check 1: Independence — no edge should connect two vertices both labeled 1
    val independenceViolation = g_in.triplets.filter { triplet =>
      triplet.srcAttr == 1 && triplet.dstAttr == 1
    }.count() > 0

    if (independenceViolation) {
      println("Independence violated: found adjacent vertices both labeled 1")
      return false
    }

    // Check 2: Maximality — every vertex labeled -1 must have a neighbor labeled 1
    val neighborMIS = g_in.aggregateMessages[Boolean](
      triplet => {
        if (triplet.srcAttr == 1 && triplet.dstAttr == -1)
          triplet.sendToDst(true)
        if (triplet.dstAttr == 1 && triplet.srcAttr == -1)
          triplet.sendToSrc(true)
      },
      (a, b) => a || b
    )

    val maximalityViolation = g_in.vertices.filter {
      case (vid, attr) => attr == -1
    }.leftOuterJoin(neighborMIS).filter {
      case (_, (_, hasMISNeighborOpt)) => hasMISNeighborOpt.getOrElse(false) == false
    }.count() > 0

    if (maximalityViolation) {
      println("Maximality violated: some non-MIS vertices have no MIS neighbors")
      return false
    }

    println("Graph is a valid Maximal Independent Set (MIS)")
    true
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
