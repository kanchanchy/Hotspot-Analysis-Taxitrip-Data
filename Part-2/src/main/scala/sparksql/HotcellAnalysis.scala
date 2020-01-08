package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val attrInfo = spark.sql("select x, y, z, count(*) as attribute from pickupInfo where x >= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " group by x, y, z")
  attrInfo.createOrReplaceTempView("attrInfo")

  spark.udf.register("toSquare", (inputX: Int) => ((HotcellUtils.toSquare(inputX))))

  val sumOfAttr = spark.sql("select sum(attribute) as attrSum, sum(toSquare(attribute)) as squaredAttrSum from attrInfo")
  sumOfAttr.createOrReplaceTempView("sumOfAttr")

  val attrSum = sumOfAttr.first().getLong(0).toDouble
  val squaredAttrSum = sumOfAttr.first().getLong(1).toDouble

  val mean: Double = (attrSum/numCells)
  val std: Double = math.sqrt((squaredAttrSum / numCells).toDouble - mean * mean)

  spark.udf.register("isAdjacent", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => (HotcellUtils.isAdjacent(x1, y1, z1, x2, y2, z2)))
  val adjacencyInfo = spark.sql("select a.x as x, a.y as y, a.z as z, b.attribute as attribute from attrInfo a, attrInfo b where isAdjacent(a.x, a.y, a.z, b.x, b.y, b.z)")
  adjacencyInfo.createOrReplaceTempView("adjacencyInfo")
  val weightInfo = spark.sql("select x, y, z, sum(attribute) as weightedAttrSum, count(*) as weightSum from adjacencyInfo group by x, y, z")
  weightInfo.createOrReplaceTempView("weightInfo")

  spark.udf.register("calculateGScore", (mean:Double, std:Double, numCells:Int, weightedAttrSum:Int, weightSum:Int) => HotcellUtils.calculateGScore(mean, std, numCells, weightedAttrSum, weightSum))
  val gScoreINfo = spark.sql("select x, y, z, calculateGScore(" + mean + ", " + std + ", " + numCells + ", weightedAttrSum, weightSum) as gScore from weightInfo")
  gScoreINfo.createOrReplaceTempView("gScoreINfo")

  val finalResult = spark.sql("select x, y, z from gScoreINfo order by gScore desc limit 50").persist()
  finalResult.createOrReplaceTempView("finalResult")
  return finalResult 
}

}
