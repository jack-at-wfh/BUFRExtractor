package com.weathernexus.utilities.common.io

import zio._
import zio.test._
import zio.test.Assertion._

object CSVParserSpec extends ZIOSpecDefault {

  val basicParsingSuite =
    suite("Basic CSV Parsing")(
      test("should parse basic line with no quotes") {
        val line = "apple,banana,cherry"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "apple",
            result(1) == "banana",
            result(2) == "cherry"
          )
        }
      },

      test("should trim leading/trailing whitespace in unquoted fields") {
        val line = " field1 ,field2 , field3"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == "field2",
            result(2) == "field3"
          )
        }
      },

      test("should handle empty fields (consecutive commas)") {
        val line = "a,,b,,c"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 5,
            result(0) == "a",
            result(1) == "",
            result(2) == "b",
            result(3) == "",
            result(4) == "c"
          )
        }
      },

      test("should handle line starting and ending with comma") {
        val line = ",field1,field2,"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 4,
            result(0) == "",
            result(1) == "field1",
            result(2) == "field2",
            result(3) == ""
          )
        }
      },

      test("should handle line with only commas") {
        val line = ",,,"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 4,
            result(0) == "",
            result(1) == "",
            result(2) == "",
            result(3) == ""
          )
        }
      },

      test("should handle single field line") {
        val line = "onlyfield"
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 1,
            result(0) == "onlyfield"
          )
        }
      },

      test("should handle single field with whitespace") {
        val line = " onlyfield "
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 1,
            result(0) == "onlyfield"
          )
        }
      },

      test("should handle empty line (returns single empty string)") {
        val line = ""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 1,
            result(0) == ""
          )
        }
      }
    )

  val quotingAndEscapingSuite =
    suite("CSV Quoting and Escaping")(
      test("should handle quoted field after comma") {
        val lineWithQuoteAfterComma = """field1,"quoted field",field3"""
        CSVParser.parseLine(lineWithQuoteAfterComma).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == "quoted field",
            result(2) == "field3"
          )
        }
      },

      test("should treat quote in middle of unquoted field as literal") {
        val lineWithMidQuote = """field1,unquoted"middle,field3"""
        CSVParser.parseLine(lineWithMidQuote).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == """unquoted"middle""",
            result(2) == "field3"
          )
        }
      },

      test("should handle escaped double quotes (\"\"\"\") inside quoted field") {
        val line = """field1,"quoted ""field"" with escaped quotes",field3"""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == """quoted "field" with escaped quotes""",
            result(2) == "field3"
          )
        }
      },

      test("should preserve whitespace inside quoted fields") {
        val line = """field1," quoted field with spaces ",field3"""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == " quoted field with spaces ",
            result(2) == "field3"
          )
        }
      },

      test("should handle quoted field at start of line") {
        val line = """ "quoted field",field2,field3"""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "quoted field",
            result(1) == "field2",
            result(2) == "field3"
          )
        }
      },

      test("should handle quoted field at end of line") {
        val line = """field1,field2,"quoted field" """
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == "field2",
            result(2) == "quoted field"
          )
        }
      },

      test("should handle only quoted field on line") {
        val line = """ "only quoted field" """
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 1,
            result(0) == "only quoted field"
          )
        }
      },

      test("should handle character after closing quote then comma (malformed)") {
        val line = """field1,"quoted"A,field2"""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 3,
            result(0) == "field1",
            result(1) == """quoted"A""",
            result(2) == "field2"
          )
        }
      },

      test("should handle character after closing quote then EOL (malformed)") {
        val line = """field1,"quoted"B"""
        CSVParser.parseLine(line).map { result =>
          assertTrue(
            result.length == 2,
            result(0) == "field1",
            result(1) == """quoted"B"""
          )
        }
      }
    )

  override def spec = suite("CSVParserSpec")(
    basicParsingSuite,
    quotingAndEscapingSuite
  ).provide(CSVParser.live)
}