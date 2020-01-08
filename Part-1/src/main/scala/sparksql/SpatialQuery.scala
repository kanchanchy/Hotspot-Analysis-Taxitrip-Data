package sparksql

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  
  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {
  	try {
  		var rectangle = new Array[String](4)
        rectangle = queryRectangle.split(",")
        var rectangle_x1 = rectangle(0).trim.toDouble
        var rectangle_y1 = rectangle(1).trim.toDouble
        var rectangle_x2 = rectangle(2).trim.toDouble
        var rectangle_y2 = rectangle(3).trim.toDouble

        var point = new Array[String](2)
        point = pointString.split(",")
        var point_x = point(0).trim.toDouble
        var point_y = point(1).trim.toDouble

        var min_x = 0.0
        var min_y = 0.0
        var max_x = 0.0
        var max_y = 0.0

        if(rectangle_x1 < rectangle_x2) {
        	min_x = rectangle_x1
        	max_x = rectangle_x2
        } else {
        	min_x = rectangle_x2
        	max_x = rectangle_x1
        }

        if(rectangle_y1 < rectangle_y2) {
        	min_y = rectangle_y1
        	max_y = rectangle_y2
        } else {
        	min_y = rectangle_y2
        	max_y = rectangle_y1
        }

        if(point_x >= min_x && point_x <= max_x && point_y >= min_y && point_y <= max_y) {
        	return true
        } else {
        	return false
        }
  	} catch {
  		case e: Exception => return false
  	}
  }


  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
  	try {
  		var point1 = new Array[String](2)
        point1 = pointString1.split(",")
        var point1_x = point1(0).trim.toDouble
        var point1_y = point1(1).trim.toDouble

        var point2 = new Array[String](2)
        point2 = pointString2.split(",")
        var point2_x = point2(0).trim.toDouble
        var point2_y = point2(1).trim.toDouble

        var difference1 = point1_x - point2_x
        var difference2 = point1_y - point2_y
        var actual_distance = Math.sqrt(difference1*difference1 + difference2*difference2)
        if(actual_distance <= distance) {
        	return true
        } else {
        	return false
        }
  	} catch {
  		case e: Exception => return false
  	}
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
