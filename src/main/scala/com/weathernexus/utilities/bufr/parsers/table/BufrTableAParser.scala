package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.BufrTableA
import com.weathernexus.utilities.common.io.{CSVParser, FileRow}

trait BufrTableAParser {
  def parsePipeline: ZPipeline[Any, Throwable, FileRow, BufrTableA]
}

object BufrTableAParser {

  final case class Live(csvParser: CSVParser) extends BufrTableAParser {
    
    def parsePipeline: ZPipeline[Any, Throwable, FileRow, BufrTableA] =
      ZPipeline.mapZIO { (row: FileRow) =>
        parseFields(csvParser.parseLine(row.content).toList, row)
      }
      
    private def parseFields(fields: List[String], row: FileRow): ZIO[Any, Throwable, BufrTableA] =
      ZIO.attempt {
        if (fields.length < 2) {
          throw new IllegalArgumentException(
            s"Invalid BUFR Table A format in ${row.filename}:${row.lineNumber} - expected at least 2 fields, got ${fields.length}"
          )
        }

        val codeFigure = fields(0).toInt
        val meaning = fields(1)
        val status = if (fields.length > 2 && fields(2).nonEmpty) Some(fields(2)) else None

        BufrTableA(
          codeFigure = codeFigure,
          meaning = meaning,
          status = status,
          sourceFile = row.filename,
          lineNumber = row.lineNumber
        )
      }.mapError { e =>
        new RuntimeException(s"Parse error in ${row.filename}:${row.lineNumber} - ${e.getMessage}", e)
      }
  }

  // ZLayer that provides the service
  val live: ZLayer[CSVParser, Nothing, BufrTableAParser] =
    ZLayer.fromFunction(Live(_))

  // Convenience method for accessing the pipeline
  def parsePipeline: ZPipeline[BufrTableAParser, Throwable, FileRow, BufrTableA] =
    ZPipeline.serviceWithPipeline[BufrTableAParser](_.parsePipeline)
}