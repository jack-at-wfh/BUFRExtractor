package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.BufrTableA
import com.weathernexus.utilities.common.io.CSVParser
import com.weathernexus.utilities.common.io.FileRow

object BufrTableAParser {

  /**
   * ZPipeline: Parse FileRow instances into BufrTableA case class instances.
   * Expects CSV format with comma delimiter.
   * Uses the robust CSVParser for proper field parsing.
   *
   * @return ZPipeline that transforms FileRow into BufrTableA.
   */
  def apply(): ZPipeline[CSVParser, Throwable, FileRow, BufrTableA] =
    ZPipeline.mapZIO { (row: FileRow) =>
      for {
        fields <- CSVParser.parseLine(row.content)
        result <- ZIO.attempt {
          // The parsing logic is now wrapped in ZIO.attempt
          // to catch exceptions like NumberFormatException.
          if (fields.length < 2) {
            throw new IllegalArgumentException(
              s"Invalid BUFR Table A format in ${row.filename}:${row.lineNumber} - expected at least 2 fields, got ${fields.length}"
            )
          }

          val codeFigure = fields(0).toInt // This is where the NumberFormatException occurs
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
          // All exceptions from the ZIO.attempt block are now caught here.
          new RuntimeException(s"Parse error in ${row.filename}:${row.lineNumber} - ${e.getMessage}", e)
        }
      } yield result
    }
}