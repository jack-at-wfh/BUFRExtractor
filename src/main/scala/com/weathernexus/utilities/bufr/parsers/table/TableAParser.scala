package com.weathernexus.utilities.bufr.parsers.table

import zio._
import zio.stream._

// Table A entry - Data categories
case class TableAEntry(
  codeFigure: Int,
  meaning: String,
  status: String
) {
  def isOperational: Boolean = status.equalsIgnoreCase("Operational")
}

// Parser for Table A CSV
object TableAParser {
  
  // Parse a single CSV line into TableAEntry
  def parseTableALine(line: String): IO[TableAParseError, TableAEntry] = {
    val fields = line.split(",", -1).map(_.trim)
    
    if (fields.length < 3) {
      ZIO.fail(TableAParseError(s"Expected 3 fields, got ${fields.length}: $line"))
    } else {
      for {
        codeFigure <- ZIO.attempt(fields(0).toInt)
                        .mapError(e => TableAParseError(s"Invalid code figure '${fields(0)}': ${e.getMessage}"))
      } yield TableAEntry(
        codeFigure = codeFigure,
        meaning = fields(1),
        status = fields(2)
      )
    }
  }
  
  // Parse CSV content into Map[Int, TableAEntry]
  def parseTableAContent(csvContent: String): IO[TableAParseError, Map[Int, TableAEntry]] = {
    val lines = csvContent.split("\n").map(_.trim).filter(_.nonEmpty)
    
    if (lines.isEmpty) {
      ZIO.fail(TableAParseError("Empty CSV content"))
    } else {
      val headerLine = lines.head
      val dataLines = lines.tail
      
      // Validate header
      val expectedHeader = "CodeFigure,Meaning_en,Status"
      if (!headerLine.startsWith("CodeFigure")) {
        ZIO.fail(TableAParseError(s"Invalid header. Expected to start with 'CodeFigure', got: $headerLine"))
      } else {
        // Parse all data lines
        ZIO.foreach(dataLines.zipWithIndex) { case (line, index) =>
          parseTableALine(line)
            .mapError(e => TableAParseError(s"Line ${index + 2}: ${e.getMessage}"))
        }.map(entries => entries.map(entry => entry.codeFigure -> entry).toMap)
      }
    }
  }
  
  // Stream-based parser
  def parseTableAStream: ZPipeline[Any, TableAParseError, String, TableAEntry] =
    ZPipeline.mapZIO(parseTableALine)
  
  // Load from resource file
  def loadTableAFromResource(resourcePath: String = "/bufr-tables/table-A-BUFR.csv"): IO[TableAParseError, Map[Int, TableAEntry]] = {
    for {
      content <- ZIO.attemptBlocking {
                  val inputStream = getClass.getResourceAsStream(resourcePath)
                  if (inputStream == null) {
                    throw new RuntimeException(s"Resource not found: $resourcePath")
                  }
                  scala.io.Source.fromInputStream(inputStream).mkString
                }.mapError(e => TableAParseError(s"Failed to load resource $resourcePath: ${e.getMessage}"))
      entries <- parseTableAContent(content)
    } yield entries
  }
}

// Error type for Table A parsing
case class TableAParseError(message: String) extends Exception {
  override def getMessage: String = s"Table A Parse Error: $message"
}