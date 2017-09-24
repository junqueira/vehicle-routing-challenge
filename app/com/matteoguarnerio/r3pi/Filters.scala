package com.matteoguarnerio.r3pi

import javax.inject.Inject

import play.api.http.HttpFilters
import play.filters.cors.CORSFilter
import play.filters.gzip.GzipFilter

class Filters @Inject() (corsFilter: CORSFilter, gzipFilter: GzipFilter) extends HttpFilters {
  def filters = Seq(corsFilter, gzipFilter)
}
