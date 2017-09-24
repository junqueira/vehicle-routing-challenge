package com.matteoguarnerio.r3pi.models


import org.joda.time.{DateTime, LocalDate, Months}

case class Project(projectId: String, profitCenter: String, attribute: String, startDate: DateTime, endDate: DateTime, spentDays: Double, spendHours: Double) {

  //val spentDays: Int = Days.daysBetween(new LocalDate(startDate), new LocalDate(endDate)).getDays
  val months: Int =
    if (new LocalDate(endDate).dayOfMonth().get() - new LocalDate(startDate).dayOfMonth().get() > 15)
      Months.monthsBetween(new LocalDate(startDate).withDayOfMonth(1), new LocalDate(endDate).withDayOfMonth(1)).getMonths + 1
    else
      Months.monthsBetween(new LocalDate(startDate).withDayOfMonth(1), new LocalDate(endDate).withDayOfMonth(1)).getMonths

  // TODO: escape holiday and saturday and sunday
  //val spentWorkingDays: Int = ???

}
