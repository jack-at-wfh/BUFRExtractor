package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.BufrCodeFlag
import com.weathernexus.utilities.common.io.CSVParser
import com.weathernexus.utilities.common.io.FileRow

object BufrCodeFlagParser {

  def apply(): ZPipeline[CSVParser, Throwable, FileRow, BufrCodeFlag] =
    ZPipeline.mapZIO { (row: FileRow) =>
      for {
        fields <- CSVParser.parseLine(row.content)
        result <- ZIO.attempt {
          // This block is what used to be a for-comprehension. It's now inside ZIO.attempt
          // to catch ALL exceptions, including NumberFormatException.
          if (fields.length < 4) {
            throw new IllegalArgumentException(
              s"Invalid BUFR code flag format in ${row.filename}:${row.lineNumber} - expected at least 4 fields, got ${fields.length}"
            )
          }

          val fxy = fields(0)
          val elementName = fields(1)
          val codeFigure = fields(2).toInt // This is where the NumberFormatException occurs
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
          // Now all exceptions from the `ZIO.attempt` block are caught here.
          new RuntimeException(s"Parse error in ${row.filename}:${row.lineNumber} - ${e.getMessage}", e)
        }
      } yield result
    }
}