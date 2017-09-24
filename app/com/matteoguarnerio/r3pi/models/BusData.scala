package com.matteoguarnerio.r3pi.models

import org.joda.time.DateTime

case class BusData(
                  dongleId: String,
                  driverId: String,
                  busId: String,
                  driverPhoneId: Option[String],
                  eventTime: String,
                  lat: Double,
                  lon: Double,
                  eventId: String,
                  eventType: String,
                  speed: Option[Double],
                  heading: Option[Double],
                  fuel: Option[Double],
                  mileage: Option[Double],
                  battery: Option[Double],
                  gForce: Option[Double],
                  speedChange: Option[Double]
                ) {

  val eventDateTime: DateTime = new DateTime(eventTime)

}

case class Coordinates(
                      lat: Double,
                      lon: Double
                      )

case class SpeedMetric(
                      time: String,
                      speed: Option[Double]
                      )

case class BusDataOutput(
                        dongleId: String,
                        driverId: String,
                        busId: String,
                        driverPhoneId: Option[String],
                        tripStartTime: String,
                        tripEndTime: String,
                        handBrakes: Long,
                        speeding: Seq[SpeedMetric],
                        busStops: Seq[Coordinates],
                        fuelConsumed: Option[Double],
                        distanceCovered: Option[Double]
                        )
