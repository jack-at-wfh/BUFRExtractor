package com.bufrtools.descriptors

import zio._
import zio.stream._
import java.io.InputStream
import scala.util.{Try, Using}

// Descriptor code representation
case class DescriptorCode(f: Int, x: Int, y: Int) {
  def toFXY: String = f"$f%01d$x%02d$y%03d"
  
  def isElementDescriptor: Boolean = f == 0
  def isReplicationDescriptor: Boolean = f == 1
  def isOperatorDescriptor: Boolean = f == 2
  def isSequenceDescriptor: Boolean = f == 3
}

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

// Enhanced CSV parser that handles quoted fields and escaped commas
// object CSVParser {
//   def parseLine(line: String): Array[String] = {
//     val result = scala.collection.mutable.ArrayBuffer[String]()
//     var current = new StringBuilder()
//     var inQuotes = false
//     var i = 0
    
//     while (i < line.length) {
//       val char = line.charAt(i)
      
//       char match {
//         case '"' if i == 0 || line.charAt(i - 1) == ',' => inQuotes = true
//         case '"' if inQuotes && (i == line.length - 1 || line.charAt(i + 1) == ',') => inQuotes = false
//         case '"' if inQuotes && i < line.length - 1 && line.charAt(i + 1) == '"' =>  current.append('"'); i += 1 // Skip next quote
//         case ',' if !inQuotes =>  result += current.toString().trim; current.clear()
//         case _ =>  current.append(char)
//       }
//       i += 1
//     }
    
//     result += current.toString().trim
//     result.toArray
//   }
// }

object CSVParser {

  /**
   * Parses a single CSV line into an Array of String fields.
   * Handles quoted fields and escaped double quotes within fields ("").
   * Trims whitespace from unquoted fields.
   *
   * This implementation uses a tail-recursive approach for functional style
   * while managing the parsing state explicitly.
   *
   * @param line The CSV line to parse.
   * @return An Array of strings representing the parsed fields.
   */
  def parseLine(line: String): Array[String] = {

    // Handle empty line explicitly. An empty line should result in an empty array of fields.
    if (line.isEmpty) return Array.empty[String]

    // Tail-recursive helper function to process the line character by character.
    // @scala.annotation.tailrec ensures the compiler optimizes this recursion
    // into a loop, preventing StackOverflowErrors for long lines.
    @scala.annotation.tailrec
    def loop(
        idx: Int, // Current index in the input 'line' string
        currentFieldChars: List[Char], // Accumulator for characters in the current field (immutable)
        inQuotes: Boolean, // Flag to indicate if currently inside a quoted field
        completedFields: List[String] // Accumulator for fields that have been fully parsed (immutable)
    ): List[String] = {
      // Base case: End of the line has been reached.
      if (idx >= line.length) {
        // The last accumulated field needs to be added to the list.
        // We prepend to `completedFields` for efficiency during recursion (List.:: is O(1)),
        // so we reverse at the very end to get the correct order.
        (currentFieldChars.mkString.trim :: completedFields).reverse
      } else {
        // Get the current character at the current index.
        val char = line.charAt(idx)

        char match {
          case '"' =>
            if (inQuotes) {
              // Case 1: We are currently inside a quoted field.
              // Check if the next character is also a double quote (escaped quote: "").
              if (idx + 1 < line.length && line.charAt(idx + 1) == '"') {
                // It's an escaped double quote (""). Append a single quote to the field
                // and advance the index by 2 (to skip both quotes).
                loop(idx + 2, currentFieldChars :+ '"', inQuotes, completedFields)
              } else {
                // It's an unescaped double quote, meaning the end of the quoted field.
                // The current field is still being built, but we are no longer in quotes.
                // Advance the index by 1.
                loop(idx + 1, currentFieldChars, false, completedFields)
              }
            } else {
              // Case 2: We are NOT in quotes. This quote must be the start of a quoted field.
              // Based on the original logic: `if (i == 0 || line.charAt(i - 1) == ',')`
              // This implies quotes only begin a field. If `currentFieldChars` is not empty,
              // it means a quote appeared in the middle of an unquoted field, which is
              // often considered malformed CSV or just a literal quote character.
              // Sticking to the original logic's intent: if it's the start of a field,
              // toggle `inQuotes` to true. Otherwise, treat it as a literal character.
              if (currentFieldChars.isEmpty || (idx > 0 && line.charAt(idx - 1) == ',')) {
                // This quote marks the beginning of a quoted field.
                // Advance the index by 1, and set `inQuotes` to true.
                loop(idx + 1, currentFieldChars, true, completedFields)
              } else {
                // This quote is not at the start of a field, so treat it as a literal character.
                // Append it to the current field and advance the index by 1.
                loop(idx + 1, currentFieldChars :+ char, inQuotes, completedFields)
              }
            }

          case ',' =>
            if (inQuotes) {
              // Case 3: Comma inside quotes is part of the field content.
              // Append the comma to the current field and advance the index by 1.
              loop(idx + 1, currentFieldChars :+ char, inQuotes, completedFields)
            } else {
              // Case 4: Comma outside quotes is a delimiter.
              // Complete the current field by converting `currentFieldChars` to a String and trimming.
              // Prepend this completed field to `completedFields`.
              // Reset `currentFieldChars` for the next field.
              // Ensure `inQuotes` is false for the new field.
              // Advance the index by 1.
              val field = currentFieldChars.mkString.trim
              loop(idx + 1, List.empty[Char], false, field :: completedFields)
            }

          case _ =>
            // Case 5: Any other character.
            // Append the character to the current field and advance the index by 1.
            loop(idx + 1, currentFieldChars :+ char, inQuotes, completedFields)
        }
      }
    }

    // Initiate the recursive parsing loop with the initial state:
    // Start at index 0, with an empty current field, not in quotes, and no completed fields yet.
    loop(0, List.empty[Char], false, List.empty[String]).toArray
  }
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
                    case Some(inputStream) =>
                      Using(scala.io.Source.fromInputStream(inputStream, "UTF-8"))(_.mkString).get
                    case None =>
                      throw new RuntimeException(s"Resource not found: $resourcePath")
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

// Companion object for DescriptorCode
object DescriptorCode {
  def fromFXY(fxyCode: String): Option[DescriptorCode] = {
    if (fxyCode.length == 6) {
      Try {
        val f = fxyCode.substring(0, 1).toInt  // F is single digit
        val x = fxyCode.substring(1, 3).toInt  // X is 2 digits
        val y = fxyCode.substring(3, 6).toInt  // Y is 3 digits
        DescriptorCode(f, x, y)
      }.toOption
    } else None
  }
  
  def apply(fxyCode: String): Option[DescriptorCode] = fromFXY(fxyCode)
}