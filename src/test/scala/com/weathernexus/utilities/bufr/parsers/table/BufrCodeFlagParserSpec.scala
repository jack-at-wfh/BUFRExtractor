package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.* 
import zio.test.*
import zio.test.Assertion.*

import com.weathernexus.utilities.common.io.*

object BufrCodeFlagParserSpec extends ZIOSpecDefault {

  def spec = suite("BufrParserPipeline")(
    test("should parse valid BUFR code flag row") {
      val row = FileRow("test.csv", 1, "001003,WMO REGION NUMBER/GEOGRAPHICAL AREA,0,ANTARCTICA,,,,")
      val pipeline = BufrCodeFlagParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.fxy)(equalTo("001003")) &&
        assert(parsed.elementName)(equalTo("WMO REGION NUMBER/GEOGRAPHICAL AREA")) &&
        assert(parsed.codeFigure)(equalTo(0)) &&
        assert(parsed.entryName)(equalTo("ANTARCTICA")) &&
        assert(parsed.entryNameSub1)(isNone) &&
        assert(parsed.entryNameSub2)(isNone) &&
        assert(parsed.note)(isNone) &&
        assert(parsed.status)(isNone) &&
        assert(parsed.sourceFile)(equalTo("test.csv")) &&
        assert(parsed.lineNumber)(equalTo(1))
      }
    },

    test("should parse row with all optional fields populated") {
      val row = FileRow("test.csv", 2, "001007,SATELLITE IDENTIFIER,3,METOP-1,Sub1,Sub2,Some note,Active")
      val pipeline = BufrCodeFlagParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.fxy)(equalTo("001007")) &&
        assert(parsed.elementName)(equalTo("SATELLITE IDENTIFIER")) &&
        assert(parsed.codeFigure)(equalTo(3)) &&
        assert(parsed.entryName)(equalTo("METOP-1")) &&
        assert(parsed.entryNameSub1)(isSome(equalTo("Sub1"))) &&
        assert(parsed.entryNameSub2)(isSome(equalTo("Sub2"))) &&
        assert(parsed.note)(isSome(equalTo("Some note"))) &&
        assert(parsed.status)(isSome(equalTo("Active")))
      }
    },

    test("should handle CSV with quoted fields containing commas") {
      val row = FileRow("test.csv", 3, """001003,"REGION, COMPLEX NAME",1,"ENTRY, WITH COMMA",,,,""")
      val pipeline = BufrCodeFlagParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.fxy)(equalTo("001003")) &&
        assert(parsed.elementName)(equalTo("REGION, COMPLEX NAME")) &&
        assert(parsed.codeFigure)(equalTo(1)) &&
        assert(parsed.entryName)(equalTo("ENTRY, WITH COMMA"))
      }
    },

    test("should handle escaped quotes in CSV fields") {
      val row = FileRow("test.csv", 4, "001003,NAME WITH \"QUOTES\",1,ENTRY,,,,")
      val pipeline = BufrCodeFlagParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.elementName)(equalTo("""NAME WITH "QUOTES""""))
      }
    },

    test("should fail for insufficient fields") {
      val row = FileRow("test.csv", 5, "001003,NAME,1") // Only 3 fields, need at least 4
      val pipeline = BufrCodeFlagParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything)))
    },

    test("should fail for invalid code figure") {
      val row = FileRow("test.csv", 6, "001003,NAME,INVALID,ENTRY,,,,")
      val pipeline = BufrCodeFlagParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything)))
    },

    test("should handle header row by failing gracefully") {
      val row = FileRow("test.csv", 1, "FXY,ElementName_en,CodeFigure,EntryName_en,EntryName_sub1_en,EntryName_sub2_en,Note_en,Status")
      val pipeline = BufrCodeFlagParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything))) // Should fail because "CodeFigure" is not a number
    },

    test("should process multiple rows") {
      val rows = List(
        FileRow("test.csv", 2, "001003,WMO REGION,0,ANTARCTICA,,,,"),
        FileRow("test.csv", 3, "001003,WMO REGION,1,REGION I,,,,"),
        FileRow("test.csv", 4, "001007,SATELLITE,1,ERS 1,,,,")
      )
      val pipeline = BufrCodeFlagParser()
      
      for {
        result <- ZStream.fromIterable(rows)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.toList
        assert(parsed)(hasSize(equalTo(3))) &&
        assert(parsed.map(_.fxy))(equalTo(List("001003", "001003", "001007"))) &&
        assert(parsed.map(_.codeFigure))(equalTo(List(0, 1, 1)))
      }
    }
  )
}