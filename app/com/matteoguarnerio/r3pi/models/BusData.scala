package com.matteoguarnerio.r3pi.models

import org.joda.time.{DateTime, LocalDate, LocalTime}

// TODO: HINT: only fuel sometimes present around 20

case class BusData(
                  dongleId: String,
                  driverId: String,
                  busId: String,
                  driverPhoneId: Option[String],
                  eventTime: String,
                  lat: Double,
                  long: Double,
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


case class BusData2(
                  dongleId: String,
                  driverId: String,
                  busId: String,
                  eventTime: String,
                  lat: Double,
                  long: Double,
                  eventId: String,
                  eventType: String,
                  speed: Double = Predef.Double2double(null)
) {
  val eventDateTime: DateTime = new DateTime(eventTime)
  val eventLocalDate: LocalDate = new LocalDate(eventDateTime)
}
