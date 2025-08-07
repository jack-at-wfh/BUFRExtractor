package com.weathernexus.utilities.bufr.parsers.table

import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import com.weathernexus.utilities.bufr.data.BufrTableB
import com.weathernexus.utilities.common.io.{FileRow, CSVParser}
import com.weathernexus.utilities.bufr.descriptors.DescriptorCode
import java.io.IOException

object BufrTableBParserSpec extends ZIOSpecDefault {

  // Test data to be used by the stream-based parser
  val testCsvContent = """ClassNo,ClassName_en,FXY,ElementName_en,Note_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Status
01,Identification,001001,WMO block number,,Numeric,0,0,7,Numeric,0,2,Operational
01,Identification,001002,WMO station number,,Numeric,0,0,10,Numeric,0,3,Operational
02,Instrumentation,002001,Type of station,,Code table,0,0,2,Code table,0,1,Operational"""

  val testCsvRows: List[FileRow] = testCsvContent.split("\n").zipWithIndex.map { case (line, idx) =>
    // Corrected the FileRow constructor call
    FileRow(content = line, lineNumber = idx + 1, filename = "test_file.csv")
  }.toList

  // A single test row for individual parsing tests
  val singleTestRow = FileRow(
    content = "01,Identification,001001,WMO block number,,Numeric,0,0,7,Numeric,0,2,Operational",
    lineNumber = 2,
    filename = "test_file.csv"
  )

  val parser = BufrTableBParser()

  val shouldParseASimpleCSVLine =
    test("should parse a simple CSV line via stream") {
      val stream = ZStream(singleTestRow).via(parser)
      for {
        entry <- stream.runHead.some
      } yield assertTrue(
        entry.classNumber == 1,
        entry.className == "Identification",
        entry.fxy == "001001",
        entry.elementName == "WMO block number",
        entry.bufrUnit == "Numeric",
        entry.bufrScale == 0,
        entry.bufrReferenceValue == 0,
        entry.bufrDataWidth == 7,
        entry.status.contains("Operational")
      )
    }

  val shouldParseMultipleEntriesFromCSVContent =
    test("should parse multiple entries from CSV content via stream") {
      val stream = ZStream.fromIterable(testCsvRows.tail).via(parser)
      for {
        entries <- stream.runCollect
      } yield assertTrue(
        entries.size == 3,
        entries.exists(_.fxy == "001001"),
        entries.exists(_.fxy == "001002"),
        entries.exists(_.fxy == "002001")
      )
    }

  val shouldHandleInvalidNumericFields =
    test("should fail on invalid numeric fields") {
      val invalidRow = FileRow(
        content = "01,Identification,001001,WMO block number,,Numeric,invalid,0,7,Numeric,0,2,Operational",
        lineNumber = 2,
        filename = "test_file.csv"
      )
      val stream = ZStream(invalidRow).via(parser)

      assertZIO(stream.runCollect.exit)(
        fails(hasMessage(containsString("For input string: \"invalid\"")))
      )
    }

  val shouldFailOnMalformedCSVLine =
    test("should fail on malformed CSV line - too few fields") {
      val malformedRow = FileRow(
        content = "01,Identification,001001,WMO block number",
        lineNumber = 2,
        filename = "test_file.csv"
      )
      val stream = ZStream(malformedRow).via(parser)

      assertZIO(stream.runCollect.exit)(
        fails(hasMessage(containsString("expected at least 9 fields")))
      )
    }

  val shouldHandleEmptyOptionalFields =
    test("should handle empty optional fields") {
      val emptyFieldsRow = FileRow(
        content = "01,Identification,001001,WMO block number,,Numeric,0,0,7,,,,",
        lineNumber = 2,
        filename = "test_file.csv"
      )
      val stream = ZStream(emptyFieldsRow).via(parser)

      for {
        entry <- stream.runHead.some
      } yield assertTrue(
        entry.note.isEmpty,
        entry.crexUnit.isEmpty,
        entry.crexScale.isEmpty,
        entry.crexDataWidth.isEmpty,
        entry.status.isEmpty
      )
    }

  val shouldHandleQuotedFieldsWithCommas =
    test("should handle quoted fields with commas") {
      val quotedRow = FileRow(
        content = """01,"Identification, extended",001001,"WMO block number, primary",,Numeric,0,0,7,Numeric,0,2,Operational""",
        lineNumber = 2,
        filename = "test_file.csv"
      )
      val stream = ZStream(quotedRow).via(parser)

      for {
        entry <- stream.runHead.some
      } yield assertTrue(
        entry.className == "Identification, extended",
        entry.elementName == "WMO block number, primary"
      )
    }

  val shouldHandleCorruptedContentInStream =
    test("should handle corrupted content in a stream") {
      val csvLines = List(
        "01,Identification,001001,WMO block number,,Numeric,0,0,7,Numeric,0,2,Operational",
        "01,Identification,001002,WMO station number,,Numeric,invalid,0,10,Numeric,0,3,Operational" // Corrupted line
      )
      val rows = csvLines.zipWithIndex.map { case (line, idx) =>
        // Corrected the FileRow constructor call
        FileRow(line, idx + 1, "test_file.csv")
      }

      val stream = ZStream.fromIterable(rows).via(parser)

      assertZIO(stream.runCollect.exit)(
        fails(isSubtype[RuntimeException](anything))
      )
    }

  val shouldParseNoteField =
    test("should correctly parse the optional 'note' field when it exists") {
      val testRowWithNote = FileRow(
        // A simple CSV line with a note in the fifth column
        content = "01,Identification,001001,WMO block number,This is a test note,Numeric,0,0,7,Numeric,0,2,Operational",
        lineNumber = 2,
        filename = "test_file.csv"
      )
      val stream = ZStream(testRowWithNote).via(parser)

      for {
        entry <- stream.runHead.some
      } yield assertTrue(
        entry.note.contains("This is a test note")
      )
    }    
  def spec = suite("BufrTableBParserSpec")(
    shouldParseASimpleCSVLine,
    shouldParseMultipleEntriesFromCSVContent,
    shouldFailOnMalformedCSVLine,
    shouldHandleInvalidNumericFields,
    shouldHandleEmptyOptionalFields,
    shouldHandleQuotedFieldsWithCommas,
    shouldHandleCorruptedContentInStream,
    shouldParseNoteField
  ).provide(
    CSVParser.live
    )
}