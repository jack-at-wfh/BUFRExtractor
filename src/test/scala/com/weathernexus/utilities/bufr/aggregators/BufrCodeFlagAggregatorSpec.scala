package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import com.weathernexus.utilities.bufr.parsers.*

object BufrCodeFlagAggregatorSpec extends ZIOSpecDefault {

  // Test data factory method
  private def createFlag(
    fxy: String = "001001",
    elementName: String = "WMO block number",
    codeFigure: Int = 0,
    entryName: String = "Reserved",
    entryNameSub1: Option[String] = None,
    entryNameSub2: Option[String] = None,
    note: Option[String] = None,
    status: Option[String] = None,
    sourceFile: String = "test.csv",
    lineNumber: Int = 1
  ): BufrCodeFlag = BufrCodeFlag(
    fxy, elementName, codeFigure, entryName, entryNameSub1, entryNameSub2, note, status, sourceFile, lineNumber
  )

  def spec = suite("BufrCodeFlagAggregatorSpec")(
    
    suite("BufrCodeFlagKey tests")(
      test("should create key from string representation") {
        val key = BufrCodeFlagKey("001001", 42)
        assert(key.asString)(equalTo("001001-42")) &&
        assert(key.toString)(equalTo("001001-42"))
      },

      test("should parse key from string") {
        val parsed = BufrCodeFlagKey.fromString("001001-42")
        assert(parsed)(isSome(equalTo(BufrCodeFlagKey("001001", 42))))
      },

      test("should handle invalid string formats") {
        assert(BufrCodeFlagKey.fromString("invalid"))(isNone) &&
        assert(BufrCodeFlagKey.fromString("001001-abc"))(isNone) &&
        assert(BufrCodeFlagKey.fromString(""))(isNone)
      },

      test("should create key from BufrCodeFlag") {
        val flag = createFlag(fxy = "002011", codeFigure = 255)
        val key = BufrCodeFlagKey.fromFlag(flag)
        assert(key)(equalTo(BufrCodeFlagKey("002011", 255)))
      }
    ),

    suite("Type-safe aggregation tests")(
      test("aggregateToMapTyped should handle empty stream") {
        for {
          result <- ZStream.empty
            .run(BufrCodeFlagAggregator.aggregateToMapTyped())
        } yield assert(result)(isEmpty)
      },

      test("aggregateToMapTyped should aggregate single flag") {
        val flag = createFlag(fxy = "001001", codeFigure = 0, entryName = "Reserved")
        val expectedKey = BufrCodeFlagKey("001001", 0)
        
        for {
          result <- ZStream(flag)
            .run(BufrCodeFlagAggregator.aggregateToMapTyped())
        } yield {
          assert(result)(hasSize(equalTo(1))) &&
          assert(result.get(expectedKey))(isSome) &&
          assert(result.get(expectedKey).get.head.entryName)(equalTo("Reserved"))
        }
      },

      test("aggregateToMapTypedSorted should sort entries by line number") {
        val flag1 = createFlag(fxy = "001001", codeFigure = 0, entryName = "Third", lineNumber = 15)
        val flag2 = createFlag(fxy = "001001", codeFigure = 0, entryName = "First", lineNumber = 5)
        val flag3 = createFlag(fxy = "001001", codeFigure = 0, entryName = "Second", lineNumber = 10)
        val expectedKey = BufrCodeFlagKey("001001", 0)
        
        for {
          result <- ZStream(flag1, flag2, flag3)
            .run(BufrCodeFlagAggregator.aggregateToMapTypedSorted())
        } yield {
          val entries = result.get(expectedKey).get
          assert(entries.map(_.entryName))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(5, 10, 15)))
        }
      },

      test("aggregateToMapTypedPreserveOrder should maintain insertion order") {
        val flag1 = createFlag(fxy = "001001", codeFigure = 0, entryName = "First", lineNumber = 10)
        val flag2 = createFlag(fxy = "001001", codeFigure = 0, entryName = "Second", lineNumber = 5)
        val flag3 = createFlag(fxy = "001001", codeFigure = 0, entryName = "Third", lineNumber = 15)
        val expectedKey = BufrCodeFlagKey("001001", 0)
        
        for {
          result <- ZStream(flag1, flag2, flag3)
            .run(BufrCodeFlagAggregator.aggregateToMapTypedPreserveOrder())
        } yield {
          val entries = result.get(expectedKey).get
          // Should maintain insertion order (First, Second, Third) regardless of line numbers
          assert(entries.map(_.entryName))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(10, 5, 15)))
        }
      },
    ),

    suite("Real BUFR data tests")(
      test("should handle real BUFR data examples") {
        val flags = List(
          createFlag(
            fxy = "001003",
            elementName = "WMO REGION NUMBER/GEOGRAPHICAL AREA",
            codeFigure = 0,
            entryName = "ANTARCTICA",
            lineNumber = 1
          ),
          createFlag(
            fxy = "001007",
            elementName = "SATELLITE IDENTIFIER", 
            codeFigure = 209,
            entryName = "NOAA 18",
            lineNumber = 2
          ),
          createFlag(
            fxy = "001031",
            elementName = "GENERATING CENTRE",
            codeFigure = 34,
            entryName = "TOKYO (RSMC), JAPAN METEOROLOGICAL AGENCY",
            lineNumber = 3
          )
        )
        
        for {
          result <- ZStream.fromIterable(flags)
            .run(BufrCodeFlagAggregator.aggregateToMapTyped())
        } yield {
          assert(result)(hasSize(equalTo(3))) &&
          assert(result.get(BufrCodeFlagKey("001003", 0)).get.head.entryName)(equalTo("ANTARCTICA")) &&
          assert(result.get(BufrCodeFlagKey("001007", 209)).get.head.entryName)(equalTo("NOAA 18")) &&
          assert(result.get(BufrCodeFlagKey("001031", 34)).get.head.entryName)(equalTo("TOKYO (RSMC), JAPAN METEOROLOGICAL AGENCY"))
        }
      }
    )
  )
}