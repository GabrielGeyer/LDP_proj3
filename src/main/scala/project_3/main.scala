package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    while (remaining_vertices >= 1) {
        // To Implement
    }
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    import org.apache.spark.graphx._

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
