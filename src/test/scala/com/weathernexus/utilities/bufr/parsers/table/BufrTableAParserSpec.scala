package com.weathernexus.utilities.bufr.parsers.table

import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import com.weathernexus.utilities.common.io.*

object BufrTableAParserSpec extends ZIOSpecDefault {

  def spec = suite("BufrTableAParser")(
    test("should parse a valid BUFR Table A row with all fields") {
      val row = FileRow("table_a.csv", 1, "0,Surface data - land,Operational")
      val pipeline = BufrTableAParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.codeFigure)(equalTo(0)) &&
        assert(parsed.meaning)(equalTo("Surface data - land")) &&
        assert(parsed.status)(isSome(equalTo("Operational"))) &&
        assert(parsed.sourceFile)(equalTo("table_a.csv")) &&
        assert(parsed.lineNumber)(equalTo(1))
      }
    },

    test("should parse a valid BUFR Table A row with optional status field missing") {
      val row = FileRow("table_a.csv", 2, "13,Forecasts,")
      val pipeline = BufrTableAParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.codeFigure)(equalTo(13)) &&
        assert(parsed.meaning)(equalTo("Forecasts")) &&
        assert(parsed.status)(isNone)
      }
    },

    test("should handle CSV with quoted fields containing commas") {
      val row = FileRow("table_a.csv", 3, """21,"Radiances (satellite, measured)",Operational""")
      val pipeline = BufrTableAParser()
      
      for {
        result <- ZStream.succeed(row)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.head
        assert(parsed.codeFigure)(equalTo(21)) &&
        assert(parsed.meaning)(equalTo("Radiances (satellite, measured)")) &&
        assert(parsed.status)(isSome(equalTo("Operational")))
      }
    },

    test("should fail for insufficient fields") {
      val row = FileRow("table_a.csv", 4, "99") // Only 1 field, needs at least 2
      val pipeline = BufrTableAParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything)))
    },

    test("should fail for invalid code figure") {
      val row = FileRow("table_a.csv", 5, "INVALID,Synoptic features,Operational")
      val pipeline = BufrTableAParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything)))
    },

    test("should handle header row by failing gracefully") {
      val row = FileRow("table_a.csv", 1, "CodeFigure,Meaning_en,Status")
      val pipeline = BufrTableAParser()
      
      assertZIO(
        ZStream.succeed(row)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(isSubtype[RuntimeException](anything))) // Fails because "CodeFigure" is not a number
    },

    test("should process multiple rows") {
      val rows = List(
        FileRow("table_a.csv", 2, "0,Surface data - land,Operational"),
        FileRow("table_a.csv", 3, "1,Surface data - sea,Operational"),
        FileRow("table_a.csv", 4, "2,Vertical soundings (other than satellite),Operational")
      )
      val pipeline = BufrTableAParser()
      
      for {
        result <- ZStream.fromIterable(rows)
          .via(pipeline)
          .runCollect
      } yield {
        val parsed = result.toList
        assert(parsed)(hasSize(equalTo(3))) &&
        assert(parsed.map(_.codeFigure))(equalTo(List(0, 1, 2))) &&
        assert(parsed.map(_.meaning))(equalTo(List(
          "Surface data - land",
          "Surface data - sea",
          "Vertical soundings (other than satellite)"
        )))
      }
    }
  ).provide(CSVParser.live)
}