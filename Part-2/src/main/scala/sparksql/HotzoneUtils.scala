package sparksql

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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

}
