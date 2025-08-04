package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.BufrTableB
import com.weathernexus.utilities.common.io.CSVParser
import com.weathernexus.utilities.common.io.FileRow

object BufrTableBParser {

  /**
   * ZPipeline: Parse FileRow instances into BufrTableB case class instances.
   * Expects CSV format with comma delimiter.
   * Uses the robust CSVParser for proper field parsing.
   *
   * @return ZPipeline that transforms FileRow into BufrTableB.
   */
  def apply(): ZPipeline[Any, Throwable, FileRow, BufrTableB] =
    ZPipeline.mapZIO { (row: FileRow) =>
      ZIO.attempt {
        val fields = CSVParser.parseLine(row.content)

        // The real Table B has 13 columns. We check for a minimum of 9 required columns.
        if (fields.length < 9) {
          throw new IllegalArgumentException(
            s"Invalid BUFR Table B format in ${row.filename}:${row.lineNumber} - expected at least 9 fields, got ${fields.length}"
          )
        }

        val classNumber = fields(0).toInt
        val className = fields(1)
        val fxy = fields(2)
        val elementName = fields(3)

        // Optional fields
        val note = if (fields.length > 4 && fields(4).nonEmpty) Some(fields(4)) else None

        val bufrUnit = fields(5)
        val bufrScale = fields(6).toInt
        val bufrReferenceValue = fields(7).toInt
        val bufrDataWidth = fields(8).toInt

        // Optional CREX fields
        val crexUnit = if (fields.length > 9 && fields(9).nonEmpty) Some(fields(9)) else None
        val crexScale = if (fields.length > 10 && fields(10).nonEmpty) Some(fields(10).toInt) else None
        val crexDataWidth = if (fields.length > 11 && fields(11).nonEmpty) Some(fields(11).toInt) else None

        // Final optional field
        val status = if (fields.length > 12 && fields(12).nonEmpty) Some(fields(12)) else None

        BufrTableB(
          classNumber = classNumber,
          className = className,
          fxy = fxy,
          elementName = elementName,
          note = note,
          bufrUnit = bufrUnit,
          bufrScale = bufrScale,
          bufrReferenceValue = bufrReferenceValue,
          bufrDataWidth = bufrDataWidth,
          crexUnit = crexUnit,
          crexScale = crexScale,
          crexDataWidth = crexDataWidth,
          status = status,
          sourceFile = row.filename,
          lineNumber = row.lineNumber
        )
      }.mapError { e =>
        new RuntimeException(s"Parse error in ${row.filename}:${row.lineNumber} - ${e.getMessage}", e)
      }
    }
}