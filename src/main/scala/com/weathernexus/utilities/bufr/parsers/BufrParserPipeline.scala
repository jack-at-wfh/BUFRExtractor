package com.weathernexus.utilities.bufr.parsers

import zio.*
import zio.stream.*

import com.weathernexus.utilities.common.io.CSVParser
import com.weathernexus.utilities.common.io.FileRow



case class BufrCodeFlag(
  fxy: String,
  elementName: String,
  codeFigure: Int,
  entryName: String,
  entryNameSub1: Option[String],
  entryNameSub2: Option[String],
  note: Option[String],
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)

object BufrParserPipeline {
  
  /**
   * ZPipeline: Parse FileRow instances into BufrCodeFlag case class instances
   * Expects CSV format with comma delimiter
   * Uses the robust CSVParser for proper field parsing
   * @return ZPipeline that transforms FileRow into BufrCodeFlag
   */
  def apply(): ZPipeline[Any, Throwable, FileRow, BufrCodeFlag] =
    ZPipeline.mapZIO { (row: FileRow) =>
      ZIO.attempt {
        val fields = CSVParser.parseLine(row.content)
        
        if (fields.length < 4) {
          throw new IllegalArgumentException(
            s"Invalid BUFR code flag format in ${row.filename}:${row.lineNumber} - expected at least 4 fields, got ${fields.length}"
          )
        }
        
        val fxy = fields(0)
        val elementName = fields(1)
        val codeFigure = fields(2).toInt
        val entryName = fields(3)
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
}