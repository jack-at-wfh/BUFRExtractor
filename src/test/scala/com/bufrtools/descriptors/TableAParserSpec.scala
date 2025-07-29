package com.bufrtools.descriptors

import zio._
import zio.stream._
import zio.test._

object TableAParserSpec extends ZIOSpecDefault {

  val validCsvContent = 
    """CodeFigure,Meaning_en,Status
      |0,Surface data - land,Operational
      |1,Surface data - sea,Operational
      |2,Vertical soundings (other than satellite),Operational
      |3,Vertical soundings (satellite),Operational""".stripMargin

  val csvWithWhitespace = 
    """CodeFigure,Meaning_en,Status
      |  0  , Surface data - land , Operational 
      |1,Surface data - sea,Operational""".stripMargin

  val invalidHeaderCsv = 
    """Code,Description,Status
      |0,Surface data - land,Operational""".stripMargin

  val invalidCodeFigureCsv = 
    """CodeFigure,Meaning_en,Status
      |abc,Surface data - land,Operational""".stripMargin

  val insufficientFieldsCsv = 
    """CodeFigure,Meaning_en,Status
      |0,Surface data - land""".stripMargin

  val emptyCsv = ""

  val shouldParseValidTableALine = 
    test("should parse valid Table A CSV line") {
      for {
        result <- TableAParser.parseTableALine("0,Surface data - land,Operational")
      } yield assertTrue(
        result.codeFigure == 0 &&
        result.meaning == "Surface data - land" &&
        result.status == "Operational" &&
        result.isOperational == true
      )
    }

  val shouldParseLineWithWhitespace = 
    test("should parse line with whitespace correctly") {
      for {
        result <- TableAParser.parseTableALine("  1  , Surface data - sea , Operational ")
      } yield assertTrue(
        result.codeFigure == 1 &&
        result.meaning == "Surface data - sea" &&
        result.status == "Operational"
      )
    }

  val shouldFailWithInsufficientFields = 
    test("should fail when line has insufficient fields") {
      for {
        result <- TableAParser.parseTableALine("0,Surface data - land").flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Expected 3 fields, got 2")
        case _ => false
      })
    }

  val shouldFailWithInvalidCodeFigure = 
    test("should fail when code figure is not a number") {
      for {
        result <- TableAParser.parseTableALine("abc,Surface data - land,Operational").flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Invalid code figure 'abc'")
        case _ => false
      })
    }

  val shouldParseValidCsvContent = 
    test("should parse valid CSV content into Map") {
      for {
        result <- TableAParser.parseTableAContent(validCsvContent)
      } yield assertTrue(
        result.size == 4 &&
        result(0).meaning == "Surface data - land" &&
        result(1).meaning == "Surface data - sea" &&
        result(2).meaning == "Vertical soundings (other than satellite)" &&
        result(3).meaning == "Vertical soundings (satellite)" &&
        result.values.forall(_.isOperational)
      )
    }

  val shouldHandleWhitespaceInCsvContent = 
    test("should handle whitespace in CSV content") {
      for {
        result <- TableAParser.parseTableAContent(csvWithWhitespace)
      } yield assertTrue(
        result.size == 2 &&
        result(0).meaning == "Surface data - land" &&
        result(1).meaning == "Surface data - sea"
      )
    }

  val shouldFailWithInvalidHeader = 
    test("should fail with invalid header") {
      for {
        result <- TableAParser.parseTableAContent(invalidHeaderCsv).flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Invalid header")
        case _ => false
      })
    }

  val shouldFailWithInvalidCodeFigureInContent = 
    test("should fail with invalid code figure in content") {
      for {
        result <- TableAParser.parseTableAContent(invalidCodeFigureCsv).flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Line 2") && msg.contains("Invalid code figure 'abc'")
        case _ => false
      })
    }

  val shouldFailWithInsufficientFieldsInContent = 
    test("should fail with insufficient fields in content") {
      for {
        result <- TableAParser.parseTableAContent(insufficientFieldsCsv).flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Line 2") && msg.contains("Expected 3 fields, got 2")
        case _ => false
      })
    }

  val shouldFailWithEmptyCsv = 
    test("should fail with empty CSV") {
      for {
        result <- TableAParser.parseTableAContent(emptyCsv).flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Empty CSV content")
        case _ => false
      })
    }

  val isOperationalShouldWorkCorrectly = 
    test("isOperational should work correctly for different statuses") {
      val operational = TableAEntry(0, "Test", "Operational")
      val preOperational = TableAEntry(1, "Test", "Pre-operational")
      val caseInsensitive = TableAEntry(2, "Test", "OPERATIONAL")
      
      assertTrue(
        operational.isOperational == true &&
        preOperational.isOperational == false &&
        caseInsensitive.isOperational == true
      )
    }

  val parseTableAStreamShouldProcessLines = 
    test("parseTableAStream should process individual lines") {
      val inputStream = ZStream("0,Surface data - land,Operational", "1,Surface data - sea,Operational")
      
      for {
        results <- inputStream
          .via(TableAParser.parseTableAStream)
          .runCollect
      } yield assertTrue(
        results.size == 2 &&
        results(0).codeFigure == 0 &&
        results(0).meaning == "Surface data - land" &&
        results(1).codeFigure == 1 &&
        results(1).meaning == "Surface data - sea"
      )
    }

  val parseTableAStreamShouldFailOnInvalidLine = 
    test("parseTableAStream should fail on invalid line") {
      val inputStream = ZStream("invalid,line")
      
      for {
        result <- inputStream
          .via(TableAParser.parseTableAStream)
          .runCollect
          .flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Expected 3 fields, got 2")
        case _ => false
      })
    }

  val tableAParseErrorShouldFormatMessageCorrectly = 
    test("TableAParseError should format message correctly") {
      val error = TableAParseError("Test error message")
      assertTrue(error.getMessage == "Table A Parse Error: Test error message")
    }

  val loadTableAFromResourceShouldFailWithBogusPath = 
    test("loadTableAFromResource should fail with bogus resource path") {
      for {
        result <- TableAParser.loadTableAFromResource("/nonexistent/path.csv").flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Failed to load resource") && msg.contains("/nonexistent/path.csv")
        case _ => false
      })
    }

  val loadTableAFromResourceShouldLoadActualFile = 
    test("loadTableAFromResource should load actual table A file") {
      for {
        result <- TableAParser.loadTableAFromResource("/bufr-tables/table-A-BUFR.csv")
      } yield assertTrue(
        result.nonEmpty &&
        result.contains(0) &&  // Should have surface data - land
        result.contains(1) &&  // Should have surface data - sea
        result.keys.forall(code => code >= 0 && code <= 255)  // All codes should be in valid range
      )
    }

  def spec = suite("TableAParser")(
    shouldParseValidTableALine,
    shouldParseLineWithWhitespace,
    shouldFailWithInsufficientFields,
    shouldFailWithInvalidCodeFigure,
    shouldParseValidCsvContent,
    shouldHandleWhitespaceInCsvContent,
    shouldFailWithInvalidHeader,
    shouldFailWithInvalidCodeFigureInContent,
    shouldFailWithInsufficientFieldsInContent,
    shouldFailWithEmptyCsv,
    isOperationalShouldWorkCorrectly,
    parseTableAStreamShouldProcessLines,
    parseTableAStreamShouldFailOnInvalidLine,
    tableAParseErrorShouldFormatMessageCorrectly,
    loadTableAFromResourceShouldFailWithBogusPath,
    loadTableAFromResourceShouldLoadActualFile
  )
}