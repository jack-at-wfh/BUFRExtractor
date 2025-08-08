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

    suite("Aggregator service tests")(
      test("should aggregate using service") {
        val entries = List(
          createTableA(codeFigure = 0, meaning = "Surface data - land", lineNumber = 1),
          createTableA(codeFigure = 11, meaning = "BUFR tables", lineNumber = 2),
          createTableA(codeFigure = 21, meaning = "Radiances", lineNumber = 3)
        )
        
        for {
          aggregator <- ZIO.service[BufrTableAAggregator]
          result <- ZStream.fromIterable(entries)
            .run(aggregator.aggregateToMap())
        } yield {
          assert(result)(hasSize(equalTo(3))) &&
          assert(result.get(BufrTableAKey(0)).get.head.meaning)(equalTo("Surface data - land")) &&
          assert(result.get(BufrTableAKey(11)).get.head.meaning)(equalTo("BUFR tables")) &&
          assert(result.get(BufrTableAKey(21)).get.head.meaning)(equalTo("Radiances"))
        }
      },

      test("should aggregate using convenience method") {
        val entries = List(
          createTableA(codeFigure = 0, meaning = "Surface data - land", lineNumber = 1),
          createTableA(codeFigure = 11, meaning = "BUFR tables", lineNumber = 2)
        )
        
        for {
          result <- ZStream.fromIterable(entries)
            .run(BufrTableAAggregator.aggregateToMap())
        } yield {
          assert(result)(hasSize(equalTo(2))) &&
          assert(result.get(BufrTableAKey(0)).get.head.meaning)(equalTo("Surface data - land")) &&
          assert(result.get(BufrTableAKey(11)).get.head.meaning)(equalTo("BUFR tables"))
        }
      },

      test("should handle multiple entries with same key") {
        val entries = List(
          createTableA(codeFigure = 0, meaning = "First entry", sourceFile = "file1.csv", lineNumber = 1),
          createTableA(codeFigure = 0, meaning = "Second entry", sourceFile = "file2.csv", lineNumber = 2),
          createTableA(codeFigure = 0, meaning = "Third entry", sourceFile = "file3.csv", lineNumber = 3)
        )
        
        for {
          aggregator <- ZIO.service[BufrTableAAggregator]
          result <- ZStream.fromIterable(entries)
            .run(aggregator.aggregateToMap())
        } yield {
          val entriesForKey = result.get(BufrTableAKey(0)).get
          assert(result)(hasSize(equalTo(1))) &&
          assert(entriesForKey)(hasSize(equalTo(3))) &&
          // Entries are in reverse order (last inserted first)
          assert(entriesForKey.head.meaning)(equalTo("Third entry")) &&
          assert(entriesForKey(1).meaning)(equalTo("Second entry")) &&
          assert(entriesForKey(2).meaning)(equalTo("First entry"))
        }
      },

      test("should handle empty stream") {
        for {
          aggregator <- ZIO.service[BufrTableAAggregator]
          result <- ZStream.empty
            .run(aggregator.aggregateToMap())
        } yield {
          assert(result)(isEmpty)
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
          aggregator <- ZIO.service[BufrTableAAggregator]
          result <- ZStream.fromIterable(entries)
            .run(aggregator.aggregateToMap())
        } yield {
          assert(result)(hasSize(equalTo(3))) &&
          assert(result.get(BufrTableAKey(0)).get.head.meaning)(equalTo("Surface data - land")) &&
          assert(result.get(BufrTableAKey(11)).get.head.meaning)(equalTo("BUFR tables, complete replacement or update")) &&
          assert(result.get(BufrTableAKey(21)).get.head.meaning)(equalTo("Radiances (satellite measured)"))
        }
      }
    )
  ).provide(
    BufrTableAAggregator.live
  )
}