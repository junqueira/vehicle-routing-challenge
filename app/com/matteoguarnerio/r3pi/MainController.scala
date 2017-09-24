package com.matteoguarnerio.r3pi

import com.matteoguarnerio.r3pi.models.{BusDataOutput, Coordinates, SpeedMetric}
import com.matteoguarnerio.r3pi.spark.SparkOperations
import play.api.mvc.{Action, AnyContent, Controller}
import play.api.libs.json.{Json, Writes}

object MainController extends Controller {

  def dummyForOptions(path: String): Action[AnyContent] = Action {
    Ok("")
  }

  def trips: Action[AnyContent] = Action {
    Ok(
      Json.toJson(SparkOperations.output.collect())
    )
  }

  implicit val speedMetricWriter = new Writes[SpeedMetric] {
    def writes(sm: SpeedMetric) = Json.obj(
      "time" -> sm.time,
      "speed" -> sm.speed
    )
  }

  implicit val CoordinatesWriter = new Writes[Coordinates] {
    def writes(c: Coordinates) = Json.obj(
      "lat" -> c.lat,
      "long" -> c.lon
    )
  }

  implicit val busDataOutputWriter = new Writes[BusDataOutput] {
    def writes(bdo: BusDataOutput) = Json.obj(
      "dongleId" -> bdo.dongleId,
      "driverId" -> bdo.driverId,
      "busId" -> bdo.busId,
      "driverPhoneId" -> bdo.driverPhoneId,
      "tripStartTime" -> bdo.tripStartTime,
      "tripEndTime" -> bdo.tripEndTime,
      "handBrakes" -> bdo.handBrakes,
      "speeding" -> bdo.speeding,
      "busStops" -> bdo.busStops,
      "fuelConsumed" -> bdo.fuelConsumed,
      "distanceCovered" -> bdo.distanceCovered
    )
  }

}
