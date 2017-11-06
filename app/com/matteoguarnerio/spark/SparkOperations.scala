package com.matteoguarnerio.spark

import com.matteoguarnerio.SparkCommons
import com.matteoguarnerio.models.{BusData, BusDataOutput, Coordinates, SpeedMetric}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SparkOperations {

  private val inputJsonPath: String = "resources/input/input.json"

  private val schema = StructType(Array(
    StructField("dongleId", StringType, nullable = false),
    StructField("driverId", StringType, nullable = false),
    StructField("busId", StringType, nullable = false),
    StructField("driverPhoneId", StringType, nullable = true),
    StructField("eventTime", StringType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("long", DoubleType, nullable = false),
    StructField("eventId", StringType, nullable = false),
    StructField("eventType", StringType, nullable = false),
    StructField("speed", DoubleType, nullable = true),
    StructField("heading", DoubleType, nullable = true),
    StructField("fuel", DoubleType, nullable = true),
    StructField("mileage", DoubleType, nullable = true),
    StructField("battery", DoubleType, nullable = true),
    StructField("gForce", DoubleType, nullable = true),
    StructField("speedChange", DoubleType, nullable = true)
  ))

  private val inputDF: DataFrame = SparkCommons.sparkSession
    .read
    .schema(schema)
    .json(inputJsonPath)

  inputDF.printSchema()
  println("Records: " + inputDF.count())

  private val busDataRDD: RDD[(String, Seq[BusData])] = inputDF.rdd
    .map {
      case r: GenericRowWithSchema => {
          val dongleId = r.getAs("dongleId").asInstanceOf[String]
          val driverId = r.getAs("driverId").asInstanceOf[String]
          val busId = r.getAs("busId").asInstanceOf[String]
          val eventTime = r.getAs("eventTime").asInstanceOf[String]
          val lat = r.getAs("lat").asInstanceOf[Double]
          val long = r.getAs("long").asInstanceOf[Double]
          val eventId = r.getAs("eventId").asInstanceOf[String]
          val eventType = r.getAs("eventType").asInstanceOf[String]

          var d: Option[String] = None
          var s: Option[Double] = None
          var h: Option[Double] = None
          var f: Option[Double] = None
          var m: Option[Double] = None
          var b: Option[Double] = None
          var g: Option[Double] = None
          var sc: Option[Double] = None

          try {
            d = if (r.isNullAt(r.fieldIndex("driverPhoneId"))) None: Option[String] else Some(r.getAs("driverPhoneId").asInstanceOf[String])
            s = if (r.isNullAt(r.fieldIndex("speed"))) None: Option[Double] else Some(r.getAs("speed").asInstanceOf[Double])
            h = if (r.isNullAt(r.fieldIndex("heading"))) None: Option[Double] else Some(r.getAs("heading").asInstanceOf[Double])
            f = if (r.isNullAt(r.fieldIndex("fuel"))) None: Option[Double] else Some(r.getAs("fuel").asInstanceOf[Double])
            m = if (r.isNullAt(r.fieldIndex("mileage"))) None: Option[Double] else Some(r.getAs("mileage").asInstanceOf[Double])
            b = if (r.isNullAt(r.fieldIndex("battery"))) None: Option[Double] else Some(r.getAs("battery").asInstanceOf[Double])
            g = if (r.isNullAt(r.fieldIndex("gForce"))) None: Option[Double] else Some(r.getAs("gForce").asInstanceOf[Double])
            sc = if (r.isNullAt(r.fieldIndex("speedChange"))) None: Option[Double] else Some(r.getAs("speedChange").asInstanceOf[Double])
          } catch {
            case _: Throwable => None
          }

          BusData(
            dongleId,
            driverId,
            busId,
            d,
            eventTime,
            lat,
            long,
            eventId,
            eventType,
            s,
            h,
            f,
            m,
            b,
            g,
            sc
          )
      }
    }
    .sortBy(_.eventTime)
    .groupBy(_.busId)
//      .map {
//        case (busId: String, busDatas) => {
//          (busId, busDatas.toSeq)
//        }
//      }
    // Calculate average speed for all the missing measurements
    .map {
      case (busId: String, busDatas) => {
        val comparison: Iterable[(BusData, BusData)] =  busDatas zip busDatas.drop(1)

        val tmp: Seq[BusData] = comparison
          .map(
            c => {
              val dist = distanceOnGeoid(c._1.lat, c._1.lon, c._2.lat, c._2.lon)
              val timeDiff = (c._2.eventDateTime.getMillis - c._1.eventDateTime.getMillis) / 1000.0
              val speedMps = dist / timeDiff
              val speedKph: Double = (speedMps * 3600.0) / 1000.0

              var speed: Option[Double] = c._1.speed
              speed = c._1.speed match {
                case Some(d) => c._1.speed
                case None => if (!speedKph.isNaN) Option(speedKph) else None
              }

              BusData(
                c._1.dongleId,
                c._1.driverId,
                c._1.busId,
                c._1.driverPhoneId,
                c._1.eventTime,
                c._1.lat,
                c._1.lon,
                c._1.eventId,
                c._1.eventType,
                speed,
                c._1.heading,
                c._1.fuel,
                c._1.mileage,
                c._1.battery,
                c._1.gForce,
                c._1.speedChange
              )
            }
          ).toSeq

        (busId, tmp :+ busDatas.last)
      }
    }
    .cache()

  val output: RDD[BusDataOutput] = busDataRDD
    .map {
      r => {
        val busDatas = r._2

        val countTotHardBrakes: Long = busDatas.count(b => {
          val g: Double = b.gForce match {
            case Some(v) => v
            case None => 0
          }
          g < -0.5F
        })

        val speedingInstances: Seq[SpeedMetric] = busDatas
          .map(b => SpeedMetric(b.eventTime, b.speed))
          .filter(b => b.speed.nonEmpty)

        val busStops: Iterable[Coordinates] = busDatas
          .filter( b => b.speed.nonEmpty && b.speed.head == 0)
          .map(b => Coordinates(b.lat, b.lon))

        val fuelDatas = busDatas.flatMap(b => b.fuel)
        var fuelConsumption: Option[Double] = None
        fuelConsumption = if (fuelDatas.nonEmpty) Some(fuelDatas.max - fuelDatas.min) else None

        val mileageDatas = busDatas.head.mileage ++ busDatas.last.mileage
        var distanceCovered: Option[Double] = None
        if (mileageDatas.nonEmpty) {
          distanceCovered = Some(mileageDatas.last - mileageDatas.head)
        }

        val dPId: Option[String] = busDatas.head.driverPhoneId

        val tripStartTime: String = busDatas.head.eventTime
        val tripEndTime: String = busDatas.last.eventTime

        BusDataOutput(
          busDatas.head.dongleId,
          busDatas.head.driverId,
          r._1,
          dPId,
          tripStartTime,
          tripEndTime,
          countTotHardBrakes,
          speedingInstances,
          busStops.toSeq,
          fuelConsumption,
          distanceCovered
        )
      }
    }
    .cache()

  private def distanceOnGeoid(lat1d: Double, lon1d: Double, lat2d: Double, lon2d: Double): Double = {
    // Convert degrees to radians
    val lat1: Double = lat1d * math.Pi / 180.0
    val lon1: Double = lon1d * math.Pi / 180.0
    val lat2: Double = lat2d * math.Pi / 180.0
    val lon2: Double = lon2d * math.Pi / 180.0
    // Radius of Earth in metres
    val r: Double = 6378100
    // P
    val rho1: Double = r * math.cos(lat1)
    val z1: Double = r * math.sin(lat1)
    val x1: Double = rho1 * math.cos(lon1)
    val y1: Double = rho1 * math.sin(lon1)
    // Q
    val rho2: Double = r * math.cos(lat2)
    val z2: Double = r * math.sin(lat2)
    val x2: Double = rho2 * math.cos(lon2)
    val y2: Double = rho2 * math.sin(lon2)
    // Dot product
    val dot: Double = x1 * x2 + y1 * y2 + z1 * z2
    val cos_theta: Double = dot / (r * r)
    val theta: Double = math.acos(cos_theta)
    // Distance in Metres
    r * theta
  }

}
