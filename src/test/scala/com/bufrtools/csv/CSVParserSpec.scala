package com.bufrtools.csv

import zio._
import zio.test._
import zio.test.Assertion._


object CSVParserSpec extends ZIOSpecDefault {

  // --- Existing Tests (slightly rephrased for clarity in suite grouping) ---

  val shouldHandleQuoteAtStartOfField =
    test("should handle quoted field after comma") {
      val lineWithQuoteAfterComma = """field1,"quoted field",field3"""
      val result = CSVParser.parseLine(lineWithQuoteAfterComma)

      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == "quoted field",
        result(2) == "field3"
      )
    }

  val shouldHandleQuoteInMiddleOfUnquotedField =
    test("should treat quote in middle of unquoted field as literal") {
      val lineWithMidQuote = """field1,unquoted"middle,field3"""
      val result = CSVParser.parseLine(lineWithMidQuote)

      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == """unquoted"middle""", // The quote is treated as part of the string
        result(2) == "field3"
      )
    }

  // --- New Test Cases ---

  val shouldParseBasicLineNoQuotes =
    test("should parse basic line with no quotes") {
      val line = "apple,banana,cherry"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "apple",
        result(1) == "banana",
        result(2) == "cherry"
      )
    }

  val shouldTrimWhitespaceInUnquotedFields =
    test("should trim leading/trailing whitespace in unquoted fields") {
      val line = "  field1  ,field2 , field3"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == "field2",
        result(2) == "field3"
      )
    }

  val shouldHandleEmptyFields =
    test("should handle empty fields (consecutive commas)") {
      val line = "a,,b,,c"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 5,
        result(0) == "a",
        result(1) == "",
        result(2) == "b",
        result(3) == "",
        result(4) == "c"
      )
    }

  val shouldHandleLineStartingAndEndingWithComma =
    test("should handle line starting and ending with comma") {
      val line = ",field1,field2,"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 4,
        result(0) == "",
        result(1) == "field1",
        result(2) == "field2",
        result(3) == ""
      )
    }

  val shouldHandleLineWithOnlyCommas =
    test("should handle line with only commas") {
      val line = ",,,"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 4,
        result(0) == "",
        result(1) == "",
        result(2) == "",
        result(3) == ""
      )
    }

  val shouldHandleSingleField =
    test("should handle single field line") {
      val line = "onlyfield"
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 1,
        result(0) == "onlyfield"
      )
    }

  val shouldHandleSingleFieldWithWhitespace =
    test("should handle single field with whitespace") {
      val line = "  onlyfield  "
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 1,
        result(0) == "onlyfield"
      )
    }

  val shouldHandleEmptyLine =
    test("should handle empty line (returns single empty string)") {
      val line = ""
      val result = CSVParser.parseLine(line)
      // Based on the current implementation, an empty line results in Array("")
      assertTrue(
        result.length == 1,
        result(0) == ""
      )
    }

  val shouldHandleEscapedQuotesInQuotedField =
    test("should handle escaped double quotes (\"\") inside quoted field") {
      val line = """field1,"quoted ""field"" with escaped quotes",field3"""
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == """quoted "field" with escaped quotes""",
        result(2) == "field3"
      )
    }

  val shouldPreserveWhitespaceInsideQuotedFields =
    test("should preserve whitespace inside quoted fields") {
      val line = """field1,"  quoted field with spaces  ",field3"""
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == "  quoted field with spaces  ", // Whitespace preserved
        result(2) == "field3"
      )
    }

  val shouldHandleQuotedFieldAtStartOfLine =
    test("should handle quoted field at start of line") {
      val line = """ "quoted field",field2,field3"""
      val result = CSVParser.parseLine(line)
      // Note: The current parser trims the *entire* field after extraction.
      // So, " "quoted field"" becomes "quoted field".
      assertTrue(
        result.length == 3,
        result(0) == "quoted field", // Leading space outside quotes is trimmed
        result(1) == "field2",
        result(2) == "field3"
      )
    }

  val shouldHandleQuotedFieldAtEndOfLine =
    test("should handle quoted field at end of line") {
      val line = """field1,field2,"quoted field" """
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == "field2",
        result(2) == "quoted field" // Trailing space outside quotes is trimmed
      )
    }

  val shouldHandleOnlyQuotedFieldOnLine =
    test("should handle only quoted field on line") {
      val line = """ "only quoted field" """
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 1,
        result(0) == "only quoted field" // Whitespace outside quotes is trimmed
      )
    }

  // --- NEW TEST CASES FOR MALFORMED INPUT AFTER CLOSING QUOTE ---

  val shouldHandleCharAfterClosingQuoteThenComma =
    test("should handle character after closing quote then comma (malformed)") {
      val line = """field1,"quoted"A,field2"""
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 3,
        result(0) == "field1",
        result(1) == """quoted"A""", // The " is treated as literal, 'A' appended
        result(2) == "field2"
      )
    }

  val shouldHandleCharAfterClosingQuoteThenEOL =
    test("should handle character after closing quote then EOL (malformed)") {
      val line = """field1,"quoted"B"""
      val result = CSVParser.parseLine(line)
      assertTrue(
        result.length == 2,
        result(0) == "field1",
        result(1) == """quoted"B""" // The " is treated as literal, 'B' appended
      )
    }

  // --- Test Suites ---

  val basicParsingSuite =
    suite("Basic CSV Parsing")(
      shouldParseBasicLineNoQuotes,
      shouldTrimWhitespaceInUnquotedFields,
      shouldHandleEmptyFields,
      shouldHandleLineStartingAndEndingWithComma,
      shouldHandleLineWithOnlyCommas,
      shouldHandleSingleField,
      shouldHandleSingleFieldWithWhitespace,
      shouldHandleEmptyLine
    )

  val quotingAndEscapingSuite =
    suite("CSV Quoting and Escaping")(
      shouldHandleQuoteAtStartOfField,
      shouldHandleQuoteInMiddleOfUnquotedField,
      shouldHandleEscapedQuotesInQuotedField,
      shouldPreserveWhitespaceInsideQuotedFields,
      shouldHandleQuotedFieldAtStartOfLine,
      shouldHandleQuotedFieldAtEndOfLine,
      shouldHandleOnlyQuotedFieldOnLine,
      shouldHandleCharAfterClosingQuoteThenComma, // New test
      shouldHandleCharAfterClosingQuoteThenEOL    // New test
    )

  // Combine all test suites
  override def spec = suite("CSVParserSpec")(
    basicParsingSuite,
    quotingAndEscapingSuite
  )
}
