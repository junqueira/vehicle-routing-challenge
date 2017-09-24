package com.matteoguarnerio.r3pi.spark

import java.sql.Date

import com.matteoguarnerio.r3pi.models.history.EntryHistory
import com.matteoguarnerio.r3pi.models.{BusData, BusData2, Project, ResultDays}
import com.matteoguarnerio.r3pi.{ProjectComparison, SparkCommons}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, Days, LocalDate}

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
    //.option("nullValue", "null")
    .json(inputJsonPath)
//    .cache()

  // TODO: DEBUG: remove, only for debug
  //inputDF.createOrReplaceTempView("input")
  inputDF.printSchema()
  println("Records: " + inputDF.count())
  val c = inputDF.collect()

  inputDF.show(100)


  // TODO: investigate how to map null field and pattern match (ex: scala.MatchError:)
  val busDataRDD: RDD[(String, Seq[BusData])] = inputDF.rdd
    .map {
      case Row(
        dongleId: String,
        driverId: String,
        busId: String,
        driverPhoneId: Any,
        eventTime: String,
        lat: Double,
        long: Double,
        eventId: String,
        eventType: String,
        speed: Double,
        heading: Double,
        fuel: Double,
        mileage: Double,
        battery: Double,
        gForce: Double,
        speedChange: Double
      ) => {

        var d: Option[String] = None
        var s: Option[Double] = None
        var h: Option[Double] = None
        var f: Option[Double] = None
        var m: Option[Double] = None
        var b: Option[Double] = None
        var g: Option[Double] = None
        var sc: Option[Double] = None

        try {
          d = Option(driverPhoneId.asInstanceOf[String])
          s = Option(speed.asInstanceOf[Double])
          h = Option(heading.asInstanceOf[Double])
          f = Option(fuel.asInstanceOf[Double])
          m = Option(mileage.asInstanceOf[Double])
          b = Option(battery.asInstanceOf[Double])
          g = Option(gForce.asInstanceOf[Double])
          sc = Option(speedChange.asInstanceOf[Double])
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
    .map {
      case (busId: String, busDatas) => {

        val comparison: Iterable[(BusData, BusData)] =  busDatas zip busDatas.drop(1)

        val tmp: Seq[BusData] = comparison
          .map(
            c => {
              val dist = distanceOnGeoid(c._1.lat, c._1.long, c._2.lat, c._2.long)
              val timeDiff = (c._2.eventDateTime.getMillis - c._1.eventDateTime.getMillis) / 1000.0
              val speedMps = dist / timeDiff
              val speedKph: Double = (speedMps * 3600.0) / 1000.0

              var speed: Option[Double] = c._1.speed
              speed = c._1.speed match {
                case Some(d) => c._1.speed
                case None => Option(speedKph)
              }

              BusData(
                c._1.dongleId,
                c._1.driverId,
                c._1.busId,
                c._1.driverPhoneId,
                c._1.eventTime,
                c._1.lat,
                c._1.long,
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

  val busDataMap = busDataRDD.collectAsMap()


  val output = busDataRDD
    .map {
      case Tuple2(busId, busDatas) => {

        // TODO: change with gForce
        val countTotHardBrakes: Int = busDatas.count(b => b.lat < -0.5F)

        // TODO: change with speed
        val speedingInstances: Seq[(Double, Double)] = busDatas.map(b => (b.lat, b.long)).toList.sorted

        // TODO: change first lat with speed, probably need object
        val busStops: Iterable[(Double, Double)] = busDatas.filter(b => b.lat == 0).map(b => (b.lat, b.long))

        // TODO: change to fuel
        val fuelDatas = busDatas.map(b => b.lat)
        val fuelConsumption: Double = fuelDatas.max - fuelDatas.min


      }
    }



  val distinctEvents: RelationalGroupedDataset = inputDF
    .groupBy(inputDF("busId"), inputDF("dongleId"))


  println("Records distinct eventId" + distinctEvents.count())
    //SparkCommons.sparkSession.sql("SELECT * FROM events")


  val projectsAnagrafica: DataFrame = SparkCommons.sparkSession.sql("SELECT projectId, profitCenter, areaLabel, dayHours, dayDate, attribute FROM history")
    .cache()

  val profitCentersAnagrafica: DataFrame = projectsAnagrafica.select(projectsAnagrafica("profitCenter"))
    .distinct()
    .orderBy(projectsAnagrafica("profitCenter"))
    .cache()

  val attributesAnagrafica: DataFrame = projectsAnagrafica.select(projectsAnagrafica("attribute"))
    .distinct()
    .orderBy(projectsAnagrafica("attribute"))
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


  def calculateProject(projectComparisonList: List[ProjectComparison], months: Int, personDaysRequested: Int): DataFrame = {
    val projectsDF = SparkCommons.sparkSession.sql("SELECT projectId, profitCenter, attribute, MIN(dayDate) as startDate, MAX(dayDate) as endDate, SUM(dayHours) as hours FROM history GROUP BY projectId, profitCenter, attribute ORDER BY projectId ASC")
      .cache()

    val projects = projectsDF
      .rdd
      .map {
        case Row(projectId: String, profitCenter: String, attribute: String, startDate: Date, endDate: Date, hours: Double) =>
          // TODO: end date is guessed by last entry in history.csv, but it should be open comparing with planning.csv project with on going tag
          projectId -> Project(projectId, profitCenter, attribute, new DateTime(startDate.toString), new DateTime(endDate), hours / 8, hours)
      }
      .filter(p =>
        projectComparisonList.exists(pc =>
          (pc.costCenter.orNull != null && pc.costCenter.orNull == p._2.profitCenter) ||
            (pc.attribute.orNull != null && pc.attribute.orNull == p._2.attribute))
      )
      .reduceByKey {
        case Tuple2(projectId, _) => projectId
      }
      .collectAsMap()

    val totalProjectsHours = projects.foldLeft(0D)(_ + _._2.spendHours)
    val mapHours = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Double]]()
    val areaLabels = scala.collection.mutable.ListBuffer[String]()

    val projectsAll = projectsAnagrafica.collect()

    def runner (i: Int) = Future {
      val mapHoursMonthTemp = scala.collection.mutable.Map[String, Double]()
      projects foreach (
        p => {
          val daysProjectTimeSpan = Days.daysBetween(new LocalDate(p._2.startDate), new LocalDate(p._2.endDate)).getDays

          val maxDate = new DateTime(p._2.startDate).plusDays(
            i * daysProjectTimeSpan / months +
              (if (i == months) 1
              else 0))

          val minDate = new DateTime(p._2.startDate).plusDays((i - 1) * daysProjectTimeSpan / months)

          val arrayHours = projectsAll.filter {
            case Row(projectId: String, _, _, _, dayDate: Date, _) =>
              projectId == p._1 &&
                new DateTime(dayDate).isBefore(maxDate) &&
                new DateTime(dayDate).isAfter(minDate)
          }
          .map {
            case Row(_, _, areaLabel: String, dayHours: Double, _, _) =>
              (areaLabel, dayHours * 100 / totalProjectsHours)
          }
          .groupBy(_._1).mapValues(_.map(_._2).sum).toArray

          arrayHours foreach (p1 => {
            if (mapHoursMonthTemp.contains(p1._1)) {
              mapHoursMonthTemp.put(p1._1, p1._2 + mapHoursMonthTemp(p1._1))
            } else {
              mapHoursMonthTemp.put(p1._1, p1._2)
            }
            areaLabels += p1._1
          })
        }
      )
      mapHours.put(i, mapHoursMonthTemp)
    }

    val l = List.range(1, months + 1)
    val futures = l map(i => runner(i))

    // Now you need to wait all your futures to be completed
    futures foreach(f => Await.ready(f, Duration.Inf))

    val resultPercentageDays = scala.collection.mutable.ListBuffer[ResultDays]()

    areaLabels.distinct foreach (areaLabel => {
      var percentage = 0D
      var days = 0D
      var monthDays = ArrayBuffer[Double]()
      for (i <- 1 to months) {
        if (mapHours(i).contains(areaLabel)) {
          percentage += mapHours(i)(areaLabel)
          days += personDaysRequested.toDouble * mapHours(i)(areaLabel) / 100
          monthDays += personDaysRequested.toDouble * mapHours(i)(areaLabel) / 100
        }
      }
      resultPercentageDays += ResultDays(areaLabel, percentage, days, monthDays: _*)
    })

    val resultPercentageDaysRDD = SparkCommons.sparkSession.sparkContext.parallelize(resultPercentageDays)

    resultPercentageDaysRDD.toDF
  }

  def calculateHistory(projectComparisonList: List[ProjectComparison], initDate: DateTime, endDate: DateTime): DataFrame = {
    val inProfitCenter = "('" + projectComparisonList.filter(p => p.costCenter.isDefined).map(p => p.costCenter.get).mkString("','") + "')"
    val inAttribute = "('" + projectComparisonList.filter(p => p.attribute.isDefined).map(p => p.attribute.get).mkString("','") + "')"

    val q =
      s"SELECT areaLabel, YEAR(dayDate) AS year, MONTH(dayDate) AS month, SUM(dayHours) AS hours FROM history " +
        s"WHERE dayDate > '$initDate' AND dayDate < '$endDate' " +
        (if (projectComparisonList.exists(_.costCenter.orNull != null)) s"AND (profitCenter IN $inProfitCenter " else " ") +
        (if (projectComparisonList.exists(_.costCenter.orNull != null) && projectComparisonList.exists(_.attribute.orNull != null)) "OR "
        else if (projectComparisonList.exists(_.attribute.orNull != null)) "AND ( " else " ") +
        (if (projectComparisonList.exists(_.attribute.orNull != null)) s" attribute IN $inAttribute) "
        else if (projectComparisonList.exists(_.costCenter.orNull != null)) ") " else " ") +
        s"GROUP BY areaLabel, YEAR(dayDate), MONTH(dayDate) ORDER BY YEAR(dayDate), MONTH(dayDate), areaLabel"

    val projectsDF: DataFrame = SparkCommons.sparkSession.sql(q)
    val projectsRDD: RDD[EntryHistory] = projectsDF
      .rdd
      .map {
        case Row(areaLabel: String, year: Int, month: Int, hours: Double) =>
          // TODO: end date is guessed by last entry in history.csv, but it should be open comparing with planning.csv project with on going tag
          EntryHistory(year.toString, month.toString, hours, areaLabel)
      }

    projectsRDD.toDF
  }
}
