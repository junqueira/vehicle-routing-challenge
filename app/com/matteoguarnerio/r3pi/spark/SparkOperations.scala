package com.matteoguarnerio.r3pi.spark

import com.matteoguarnerio.r3pi.SparkCommons
import com.matteoguarnerio.r3pi.models._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.Map

object SparkOperations {

  import SparkCommons.sparkSession.implicits._

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
    .cache()

  // TODO: DEBUG: remove, only for debug
  //inputDF.createOrReplaceTempView("input")
  inputDF.printSchema()
  println("Records: " + inputDF.count())

  private val busDataRDD: RDD[(String, Iterable[BusData])] = inputDF.rdd
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

  private val busDataMap: Map[String, Iterable[BusData]] = busDataRDD.collectAsMap()

  val output: RDD[BusDataOutput] = busDataRDD
    .map {
      r => {
        val busDatas: Iterable[BusData] = r._2

        val countTotHardBrakes: Long = busDatas.count(b => {
          val g: Double = b.gForce match {
            case Some(v) => v
            case None => 0
          }
          g < -0.5F
        })

        val speedingInstances: Seq[SpeedMetric] = busDatas.map(b => {
          val s: Double = b.speed match {
            case Some(v) => v
            case None => -99999999
          }
          SpeedMetric(b.eventTime, s)
        }).filter(_.speed != -99999999).toList.sortWith((l, r) => l.speed > r.speed)

        val busStops: Iterable[Coordinates] = busDatas.filter(b => {
          val s: Double = b.speed match {
            case Some(v) => v
            case None => -99999999
          }
          s == 0
        }).filter(_.speed != -99999999).map(b => Coordinates(b.lat, b.lon))

        val fuelDatas: Iterable[Double] = busDatas.flatMap(b => b.fuel)
        var fuelConsumption: Option[Double] = None
        fuelConsumption = if (fuelDatas.nonEmpty) Some(fuelDatas.max - fuelDatas.min) else None

        val mileageDatas: Iterable[Double] = busDatas.head.mileage ++ busDatas.last.mileage
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
    val lat1 = lat1d * math.Pi / 180.0
    val lon1 = lon1d * math.Pi / 180.0
    val lat2 = lat2d * math.Pi / 180.0
    val lon2 = lon2d * math.Pi / 180.0
    // Radius of Earth in metres
    val r = 6378100
    // P
    val rho1 = r * math.cos(lat1)
    val z1 = r * math.sin(lat1)
    val x1 = rho1 * math.cos(lon1)
    val y1 = rho1 * math.sin(lon1)
    // Q
    val rho2 = r * math.cos(lat2)
    val z2 = r * math.sin(lat2)
    val x2 = rho2 * math.cos(lon2)
    val y2 = rho2 * math.sin(lon2)
    // Dot product
    val dot = x1 * x2 + y1 * y2 + z1 * z2
    val cos_theta = dot / (r * r)
    val theta = math.acos(cos_theta)
    // Distance in Metres
    r * theta
  }

}
