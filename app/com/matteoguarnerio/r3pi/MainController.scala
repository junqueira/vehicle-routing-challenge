package com.matteoguarnerio.r3pi

import com.matteoguarnerio.r3pi.spark.SparkOperations
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent, Controller}

import scala.io.Source

object MainController extends Controller {

  def ready: Action[AnyContent] = Action {
    SparkOperations
    Ok("Server ready")
  }

  def ping: Action[AnyContent] = Action {
    Ok("pong")
  }

  def dummyForOptions(path: String): Action[AnyContent] = Action {
    Ok("")
  }

  def profitCenters: Action[AnyContent] = Action {
    Ok(
      toJsonString(
        SparkOperations.profitCentersAnagrafica
      )
    )
  }

  def attributes: Action[AnyContent] = Action {
    Ok(
      toJsonString(
        SparkOperations.attributesAnagrafica
      )
    )
  }

  /**
    * Calculate forecast about projects based on given months, days and type of project
    * @return
    */
  def calculateProject: Action[AnyContent] = Action { request =>
    val projectComparisonList = (request.body.asJson.get \ "projects").as[List[ProjectComparison]]
    val months = (request.body.asJson.get \ "months").as[Int]
    val days = (request.body.asJson.get \ "days").as[Int]

    Ok(
      toJsonString(
        SparkOperations.calculateProject(projectComparisonList, months, days)
      )
    )
  }

  /**
    * Calculate forecast about projects based on given months and days
    * @return
    */
  def calculateProjectTT: Action[AnyContent] = Action { request =>

    val months = (request.body.asJson.get \ "months").as[Int]
    val days = (request.body.asJson.get \ "days").as[Int]

    Ok(
      toJsonString(
        SparkOperations.calculateProject(getProjectTTParameters, months, days)
      )
    )
  }

  /**
    * Get historical data about projects grouped by month and area given a project type
    *
    * @return
    */
  def calculateCSVProjectTT: Action[AnyContent] = Action { request =>
    val months = (request.body.asJson.get \ "months").as[Int]
    val days = (request.body.asJson.get \ "days").as[Int]

    Ok(
      toCsvString(
        SparkOperations.calculateProject(getProjectTTParameters, months, days)
      )
    )
  }

  /**
    * Get historical data about projects and group by month and area by given project type
    *
    * @return
    */
  def historyProject: Action[AnyContent] = Action { request =>
    val projectComparisonList = (request.body.asJson.get \ "projects").as[List[ProjectComparison]]
    val initDate = (request.body.asJson.get \ "initDate").as[DateTime]
    val endDate = (request.body.asJson.get \ "endDate").as[DateTime]

    Ok(
      toJsonString(
        SparkOperations.calculateHistory(projectComparisonList, initDate, endDate)
      )
    )
  }

  /**
    * Get historical data about projects grouped by month and area
    * @return
    */
  def historyProjectTT: Action[AnyContent] = Action { request =>
    val initDate = (request.body.asJson.get \ "initDate").as[DateTime]
    val endDate = (request.body.asJson.get \ "endDate").as[DateTime]

    Ok(
      toJsonString(
        SparkOperations.calculateHistory(getProjectTTParameters, initDate, endDate)
      )
    )
  }

  private def getProjectTTParameters: List[ProjectComparison] = {
    val ttJsonPath: String = "resources/input/TandTProjects.json"
    (Json.parse(Source.fromFile(ttJsonPath).getLines.mkString) \ "projects").as[List[ProjectComparison]]
  }

  /**
   * DataFrame can output with toJSON a list of JSON strings. They just need to be wrapped with [] and commas
   * @param rdd
   * @return
   */
  private def toJsonString(rdd:DataFrame): String =
    "{\"response\": ["+ rdd.toJSON.collect.toList.mkString(",\n") +"]}"

  /**
    * DataFrame output in csv format
    *
    * @param df
    * @return
    */
  private def toCsvString(df: DataFrame): String =
    df.rdd.map(x => x.mkString(",")).collect().mkString("\n")
}
