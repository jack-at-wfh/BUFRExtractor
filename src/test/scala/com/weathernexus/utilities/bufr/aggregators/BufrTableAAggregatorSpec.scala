package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.bufr.parsers.*

object BufrTableAAggregatorSpec extends ZIOSpecDefault {

  // Test data factory method
  private def createTableA(
    codeFigure: Int = 0,
    meaning: String = "Surface data",
    status: Option[String] = Some("Operational"),
    sourceFile: String = "test.csv",
    lineNumber: Int = 1
  ): BufrTableA = BufrTableA(
    codeFigure, meaning, status, sourceFile, lineNumber
  )

  def spec = suite("BufrTableAAggregatorSpec")(
    
    suite("BufrTableAKey tests")(
      test("should create key from string representation") {
        val key = BufrTableAKey(42)
        assert(key.asString)(equalTo("42")) &&
        assert(key.toString)(equalTo("42"))
      },

      test("should parse key from string") {
        val parsed = BufrTableAKey.fromString("42")
        assert(parsed)(isSome(equalTo(BufrTableAKey(42))))
      },

      test("should handle invalid string formats") {
        assert(BufrTableAKey.fromString("invalid"))(isNone) &&
        assert(BufrTableAKey.fromString(""))(isNone)
      },

      test("should create key from BufrTableA") {
        val entry = createTableA(codeFigure = 255)
        val key = BufrTableAKey.fromEntry(entry)
        assert(key)(equalTo(BufrTableAKey(255)))
      }
    ),

    suite("Type-safe aggregation tests")(
      test("aggregateToMapTyped should handle empty stream") {
        for {
          result <- ZStream.empty
            .run(BufrTableAAggregator.aggregateToMapTyped())
        } yield assert(result)(isEmpty)
      },

      test("aggregateToMapTyped should aggregate single entry") {
        val entry = createTableA(codeFigure = 0, meaning = "Surface data - land")
        val expectedKey = BufrTableAKey(0)
        
        for {
          result <- ZStream(entry)
            .run(BufrTableAAggregator.aggregateToMapTyped())
        } yield {
          assert(result)(hasSize(equalTo(1))) &&
          assert(result.get(expectedKey))(isSome) &&
          assert(result.get(expectedKey).get.head.meaning)(equalTo("Surface data - land"))
        }
      },

      test("aggregateToMapTypedSorted should sort entries by line number") {
        val entry1 = createTableA(codeFigure = 15, meaning = "Third", lineNumber = 15)
        val entry2 = createTableA(codeFigure = 15, meaning = "First", lineNumber = 5)
        val entry3 = createTableA(codeFigure = 15, meaning = "Second", lineNumber = 10)
        val expectedKey = BufrTableAKey(15)
        
        for {
          result <- ZStream(entry1, entry2, entry3)
            .run(BufrTableAAggregator.aggregateToMapTypedSorted())
        } yield {
          val entries = result.get(expectedKey).get
          assert(entries.map(_.meaning))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(5, 10, 15)))
        }
      },

      test("aggregateToMapTypedPreserveOrder should maintain insertion order") {
        val entry1 = createTableA(codeFigure = 15, meaning = "First", lineNumber = 10)
        val entry2 = createTableA(codeFigure = 15, meaning = "Second", lineNumber = 5)
        val entry3 = createTableA(codeFigure = 15, meaning = "Third", lineNumber = 15)
        val expectedKey = BufrTableAKey(15)
        
        for {
          result <- ZStream(entry1, entry2, entry3)
            .run(BufrTableAAggregator.aggregateToMapTypedPreserveOrder())
        } yield {
          val entries = result.get(expectedKey).get
          // Should maintain insertion order (First, Second, Third) regardless of line numbers
          assert(entries.map(_.meaning))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(10, 5, 15)))
        }
      }
    ),

    suite("Real BUFR data tests")(
      test("should handle real BUFR Table A examples") {
        val entries = List(
          createTableA(
            codeFigure = 0,
            meaning = "Surface data - land",
            status = Some("Operational"),
            lineNumber = 1
          ),
          createTableA(
            codeFigure = 11,
            meaning = "BUFR tables, complete replacement or update",
            status = Some("Operational"),
            lineNumber = 2
          ),
          createTableA(
            codeFigure = 21,
            meaning = "Radiances (satellite measured)",
            status = Some("Operational"),
            lineNumber = 3
          )
        )
        
        for {
          result <- ZStream.fromIterable(entries)
            .run(BufrTableAAggregator.aggregateToMapTyped())
        } yield {
          assert(result)(hasSize(equalTo(3))) &&
          assert(result.get(BufrTableAKey(0)).get.head.meaning)(equalTo("Surface data - land")) &&
          assert(result.get(BufrTableAKey(11)).get.head.meaning)(equalTo("BUFR tables, complete replacement or update")) &&
          assert(result.get(BufrTableAKey(21)).get.head.meaning)(equalTo("Radiances (satellite measured)"))
        }
      }
    )
  )
}