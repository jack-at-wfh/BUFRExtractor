package com.weathernexus.utilities.bufr.parsers.table

import zio._
import zio.stream._
import java.io.InputStream
import scala.util.Using
import com.weathernexus.utilities.common.io.*
import com.weathernexus.utilities.bufr.descriptors.*


// Table B entry - Element descriptors  
case class TableBEntry(
  classNumber: String,           // Class (e.g., "01")
  classDescription: String,      // Class description (e.g., "Identification") 
  fxyCode: String,              // FXY code (e.g., "001001")
  elementName: String,          // Element name (e.g., "WMO block number")
  unit: String,                 // Unit (e.g., "Numeric", "m/s", "CCITT IA5")
  scale: Int,                   // Scale factor
  referenceValue: Long,         // Reference value (using Long for large values)
  dataWidth: Int,               // Data width in bits
  crexUnit: String,             // CREX unit (often same as unit)
  crexScale: Int,               // CREX scale
  crexDataWidth: Int,           // CREX data width
  note: Option[String],         // Optional note text
  noteNumber: Option[Int],      // Optional note number
  status: String                // Status (e.g., "Operational")
) {
  def isOperational: Boolean = status.equalsIgnoreCase("Operational")
  
  /**
   * Extracts F, X, Y components from a 6-character string code.
   * Returns Some(DescriptorCode) if parsing is successful and the string has the correct length,
   * otherwise returns None.
   */
  def descriptorCode: Option[DescriptorCode] = {
    // Use Option.when to only proceed if the length is exactly 6
    Option.when(fxyCode.length == 6) {
      // Use a for-comprehension over Option to safely parse each part.
      // If any substring fails to parse to an Int (e.g., "0A1002"),
      // toIntOption will return None, and the entire for-comprehension
      // will short-circuit to None.
      for {
        f <- fxyCode.substring(0, 1).toIntOption // F is single digit
        x <- fxyCode.substring(1, 3).toIntOption // X is 2 digits
        y <- fxyCode.substring(3, 6).toIntOption // Y is 3 digits
      } yield DescriptorCode(f, x, y)
    }.flatten // Flatten Option[Option[DescriptorCode]] to Option[DescriptorCode]
  }

  // Helper methods for data interpretation
  def actualValue(encodedValue: Long): Double = {
    (encodedValue + referenceValue) * math.pow(10, -scale)
  }
  
  def encodeValue(actualValue: Double): Long = {
    math.round(actualValue * math.pow(10, scale)) - referenceValue
  }
  
  def isNumeric: Boolean = unit.equalsIgnoreCase("Numeric")
  def isCharacter: Boolean = unit.equalsIgnoreCase("CCITT IA5") || unit.equalsIgnoreCase("Character")
  def isCodeTable: Boolean = unit.equalsIgnoreCase("Code table")
  def isFlagTable: Boolean = unit.equalsIgnoreCase("Flag table")
}

// Error type for Table B parsing
case class TableBParseError(message: String) extends Exception {
  override def getMessage: String = s"Table B Parse Error: $message"
}

// Parser for Table B CSV
object TableBParser {
  
  // Parse a single CSV line into TableBEntry
  def parseTableBLine(line: String): IO[TableBParseError, TableBEntry] = {
    val fields = CSVParser.parseLine(line)
    
    if (fields.length < 14) {
      ZIO.fail(TableBParseError(s"Expected 14 fields, got ${fields.length}: $line"))
    } else {
      for {
        scale <- ZIO.attempt(if (fields(5).isEmpty) 0 else fields(5).toInt)
                   .mapError(e => TableBParseError(s"Invalid scale '${fields(5)}': ${e.getMessage}"))
        referenceValue <- ZIO.attempt(if (fields(6).isEmpty) 0L else fields(6).toLong)
                           .mapError(e => TableBParseError(s"Invalid reference value '${fields(6)}': ${e.getMessage}"))
        dataWidth <- ZIO.attempt(if (fields(7).isEmpty) 0 else fields(7).toInt)
                      .mapError(e => TableBParseError(s"Invalid data width '${fields(7)}': ${e.getMessage}"))
        crexScale <- ZIO.attempt(if (fields(9).isEmpty) 0 else fields(9).toInt)
                      .mapError(e => TableBParseError(s"Invalid CREX scale '${fields(9)}': ${e.getMessage}"))
        crexDataWidth <- ZIO.attempt(if (fields(10).isEmpty) 0 else fields(10).toInt)
                          .mapError(e => TableBParseError(s"Invalid CREX data width '${fields(10)}': ${e.getMessage}"))
        noteNumber <- ZIO.attempt {
                       val noteStr = fields(12).trim
                       if (noteStr.isEmpty) None else Some(noteStr.toInt)
                     }.mapError(e => TableBParseError(s"Invalid note number '${fields(12)}': ${e.getMessage}"))
      } yield TableBEntry(
        classNumber = fields(0),
        classDescription = fields(1),
        fxyCode = fields(2),
        elementName = fields(3),
        unit = fields(4),
        scale = scale,
        referenceValue = referenceValue,
        dataWidth = dataWidth,
        crexUnit = fields(8),
        crexScale = crexScale,
        crexDataWidth = crexDataWidth,
        note = if (fields(11).trim.isEmpty) None else Some(fields(11)),
        noteNumber = noteNumber,
        status = fields(13)
      )
    }
  }
  
  // Parse CSV content into Map[DescriptorCode, TableBEntry]
  def parseTableBContent(csvContent: String): IO[TableBParseError, Map[DescriptorCode, TableBEntry]] = {
    val lines = csvContent.split("\n").map(_.trim).filter(_.nonEmpty)
    
    if (lines.isEmpty) {
      ZIO.fail(TableBParseError("Empty CSV content"))
    } else {
      val headerLine = lines.head
      val dataLines = lines.tail
      
      // Basic header validation (just check it starts with expected fields)
      if (!headerLine.toLowerCase.contains("class") || !headerLine.toLowerCase.contains("fxy")) {
        ZIO.fail(TableBParseError(s"Invalid header. Expected header with 'class' and 'fxy' fields, got: $headerLine"))
      } else {
        // Parse all data lines
        ZIO.foreach(dataLines.zipWithIndex) { case (line, index) =>
          parseTableBLine(line)
            .mapError(e => TableBParseError(s"Line ${index + 2}: ${e.getMessage}"))
        }.flatMap { entries =>
          // Convert to map, filtering out entries without valid descriptor codes
          val validEntries = entries.flatMap { entry =>
            entry.descriptorCode.map(_ -> entry)
          }
          
          if (validEntries.isEmpty && entries.nonEmpty) {
            ZIO.fail(TableBParseError("No valid descriptor codes found in parsed entries"))
          } else {
            ZIO.succeed(validEntries.toMap)
          }
        }
      }
    }
  }
  
  // Stream-based parser
  def parseTableBStream: ZPipeline[Any, TableBParseError, String, TableBEntry] =
    ZPipeline.mapZIO(parseTableBLine)
  
  // Load from resource file
  def loadTableBFromResource(resourcePath: String = "/bufr-tables/table-B-BUFRCREX.csv"): IO[TableBParseError, Map[DescriptorCode, TableBEntry]] = {
    for {
      content <- ZIO.attemptBlocking {
                  Option(getClass.getResourceAsStream(resourcePath)) match {
                    case Some(inputStream) => Using(scala.io.Source.fromInputStream(inputStream, "UTF-8"))(_.mkString).get
                    case None => throw new RuntimeException(s"Resource not found: $resourcePath")
                  }
                }.mapError(e => TableBParseError(s"Failed to load resource $resourcePath: ${e.getMessage}"))
      entries <- parseTableBContent(content)
    } yield entries
  }
  
  // Load from file path
  def loadTableBFromFile(filePath: String): IO[TableBParseError, Map[DescriptorCode, TableBEntry]] = {
    for {
      content <- ZIO.attemptBlocking {
                  Using(scala.io.Source.fromFile(filePath, "UTF-8"))(_.mkString).get
                }.mapError(e => TableBParseError(s"Failed to load file $filePath: ${e.getMessage}"))
      entries <- parseTableBContent(content)
    } yield entries
  }
  
  // Parse from string content
  def parseFromString(csvContent: String): IO[TableBParseError, Map[DescriptorCode, TableBEntry]] = {
    parseTableBContent(csvContent)
  }
}

// Table B lookup service
class TableBService(entries: Map[DescriptorCode, TableBEntry]) {
  
  def getEntry(descriptorCode: DescriptorCode): Option[TableBEntry] = {
    entries.get(descriptorCode)
  }
  
  def getEntry(fxyCode: String): Option[TableBEntry] = {
    DescriptorCode.fromFXY(fxyCode).flatMap(entries.get)
  }
  
  def getEntriesByClass(classNumber: String): List[TableBEntry] = {
    entries.values.filter(_.classNumber == classNumber).toList
  }
  
  def getOperationalEntries: List[TableBEntry] = {
    entries.values.filter(_.isOperational).toList
  }
  
  def searchByName(pattern: String): List[TableBEntry] = {
    val regex = pattern.r
    entries.values.filter(entry => regex.findFirstIn(entry.elementName).isDefined).toList
  }
  
  def getElementDescriptors: List[TableBEntry] = {
    entries.values.filter(_.descriptorCode.exists(_.isElementDescriptor)).toList
  }
  
  def getStatistics: TableBStatistics = {
    val allEntries = entries.values
    TableBStatistics(
      totalEntries = allEntries.size,
      operationalEntries = allEntries.count(_.isOperational),
      entriesWithNotes = allEntries.count(_.note.isDefined),
      classCounts = allEntries.groupBy(_.classNumber).view.mapValues(_.size).toMap,
      unitCounts = allEntries.groupBy(_.unit).view.mapValues(_.size).toMap
    )
  }
}

// Statistics for Table B data
case class TableBStatistics(
  totalEntries: Int,
  operationalEntries: Int,
  entriesWithNotes: Int,
  classCounts: Map[String, Int],
  unitCounts: Map[String, Int]
)