package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.BufrCodeFlag
import com.weathernexus.utilities.common.io.{CSVParser, FileRow}

trait BufrCodeFlagParser {
  def parsePipeline: ZPipeline[Any, Throwable, FileRow, BufrCodeFlag]
}

object BufrCodeFlagParser {

  final case class Live(csvParser: CSVParser) extends BufrCodeFlagParser {
    
    def parsePipeline: ZPipeline[Any, Throwable, FileRow, BufrCodeFlag] =
      ZPipeline.mapZIO { (row: FileRow) =>
        parseFields(csvParser.parseLine(row.content), row)
      }

    private def parseFields(fields: Array[String], row: FileRow): ZIO[Any, Throwable, BufrCodeFlag] =
      ZIO.attempt {
        // Expect at least 4 required fields: FXY, ElementName, CodeFigure, EntryName
        if (fields.length < 4) {
          throw new IllegalArgumentException(
            s"Invalid BUFR Code Flag format in ${row.filename}:${row.lineNumber} - expected at least 4 fields, got ${fields.length}"
          )
        }

        val fxy = fields(0)
        val elementName = fields(1)
        val codeFigure = fields(2).toInt
        val entryName = fields(3)

        // Optional fields
        val entryNameSub1 = if (fields.length > 4 && fields(4).nonEmpty) Some(fields(4)) else None
        val entryNameSub2 = if (fields.length > 5 && fields(5).nonEmpty) Some(fields(5)) else None
        val note = if (fields.length > 6 && fields(6).nonEmpty) Some(fields(6)) else None
        val status = if (fields.length > 7 && fields(7).nonEmpty) Some(fields(7)) else None

        BufrCodeFlag(
          fxy = fxy,
          elementName = elementName,
          codeFigure = codeFigure,
          entryName = entryName,
          entryNameSub1 = entryNameSub1,
          entryNameSub2 = entryNameSub2,
          note = note,
          status = status,
          sourceFile = row.filename,
          lineNumber = row.lineNumber
        )
      }.mapError { e =>
        new RuntimeException(s"Parse error in ${row.filename}:${row.lineNumber} - ${e.getMessage}", e)
      }
  }

  val live: ZLayer[CSVParser, Nothing, BufrCodeFlagParser] = 
    ZLayer.fromFunction(Live(_))

  // Convenience method for accessing the pipeline
  def parsePipeline: ZPipeline[BufrCodeFlagParser, Throwable, FileRow, BufrCodeFlag] =
    ZPipeline.serviceWithPipeline[BufrCodeFlagParser](_.parsePipeline)
}