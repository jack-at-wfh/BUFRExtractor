package com.weathernexus.utilities.common.io

import zio._

trait CSVParser {
  /**
   * Parses a single CSV line into an Array of String fields.
   */
  def parseLine(line: String): Array[String]
}

object CSVParser {

  def live: ZLayer[Any, Nothing, CSVParser] =
    ZLayer.succeed(CSVParserImpl())

  def parseLine(line: String): ZIO[CSVParser, Nothing, Array[String]] =
    ZIO.serviceWith[CSVParser](_.parseLine(line))
}