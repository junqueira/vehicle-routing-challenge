package com.matteoguarnerio.r3pi

import play.api.libs.json._
import play.api.libs.functional.syntax._

case class ProjectComparison(costCenter: Option[String], attribute: Option[String])

object ProjectComparison {

  implicit val readProjectComparison: Reads[ProjectComparison] = (
    (JsPath \ "costCenter").readNullable[String] and
      (JsPath \ "attribute").readNullable[String]
    ) (ProjectComparison.apply _)

  implicit val projectComparisonFormat = Json.format[ProjectComparison]
}
