package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.bufr.descriptors.DescriptorCode
import com.weathernexus.utilities.bufr.parsers.*

object BufrTableBAggregatorSpec extends ZIOSpecDefault {

  // Test data factory method
private def createTableB(
    fxy: String = "001001",
    elementName: String = "Test Element",
    bufrUnit: String = "Numeric",
    bufrScale: Int = 0,
    bufrReferenceValue: Int = 0,
    bufrDataWidth: Int = 8,
    status: Option[String] = Some("Operational"),
    sourceFile: String = "test.csv",
    lineNumber: Int = 1
  ): BufrTableB = BufrTableB(
    classNumber = try { fxy.substring(0, 3).toInt } catch { case _: Throwable => 0 },
    className = "Test Class",
    fxy = fxy,
    elementName = elementName,
    note = None,
    bufrUnit = bufrUnit,
    bufrScale = bufrScale,
    bufrReferenceValue = bufrReferenceValue,
    bufrDataWidth = bufrDataWidth,
    crexUnit = None,
    crexScale = None,
    crexDataWidth = None,
    status = status,
    sourceFile = sourceFile,
    lineNumber = lineNumber
  )

  def spec = suite("BufrTableBAggregatorSpec")(
    
    // Suite to test the DescriptorCode functionality, which acts as the key
    suite("DescriptorCode tests")(
      test("should create key from FXY string representation") {
        val key = DescriptorCode(0, 1, 1)
        assert(key.toFXY)(equalTo("001001")) &&
        assert(key.toString)(equalTo("001001"))
      },

      test("should parse key from FXY string") {
        val parsed = DescriptorCode.fromFXY("001001")
        assert(parsed)(isSome(equalTo(DescriptorCode(0, 1, 1))))
      },

      test("should handle invalid FXY string formats") {
        assert(DescriptorCode.fromFXY("invalid"))(isNone) &&
        assert(DescriptorCode.fromFXY("01"))(isNone)
      }
    ),

    suite("Type-safe aggregation tests")(
      test("aggregateToMapTyped should handle empty stream") {
        for {
          result <- ZStream.empty
            .run(BufrTableBAggregator.aggregateToMapTyped())
        } yield assert(result)(isEmpty)
      },

      test("aggregateToMapTyped should aggregate a single entry") {
        val entry = createTableB(fxy = "002001", elementName = "Type of station")
        val expectedKey = DescriptorCode(0, 2, 1)
        
        for {
          result <- ZStream(entry)
            .run(BufrTableBAggregator.aggregateToMapTyped())
        } yield {
          assert(result)(hasSize(equalTo(1))) &&
          assert(result.get(expectedKey))(isSome) &&
          assert(result.get(expectedKey).get.head.elementName)(equalTo("Type of station"))
        }
      },

      test("aggregateToMapTyped should aggregate multiple entries with same key") {
        val entry1 = createTableB(fxy = "001001", elementName = "WMO block number", lineNumber = 1)
        val entry2 = createTableB(fxy = "001001", elementName = "WMO block number (deprecated)", lineNumber = 5)
        val expectedKey = DescriptorCode(0, 1, 1)

        for {
          result <- ZStream(entry1, entry2)
            .run(BufrTableBAggregator.aggregateToMapTyped())
        } yield {
          val entries = result.get(expectedKey).get
          assert(entries)(hasSize(equalTo(2))) &&
          // The order should be reverse insertion, so the last one is first
          assert(entries.head.elementName)(equalTo("WMO block number (deprecated)")) &&
          assert(entries.last.elementName)(equalTo("WMO block number"))
        }
      },

      test("aggregateToMapTyped should aggregate multiple entries with different keys") {
        val entry1 = createTableB(fxy = "001001")
        val entry2 = createTableB(fxy = "001002")
        val entry3 = createTableB(fxy = "002001")
        
        for {
          result <- ZStream(entry1, entry2, entry3)
            .run(BufrTableBAggregator.aggregateToMapTyped())
        } yield assert(result)(hasSize(equalTo(3))) &&
          assert(result.contains(DescriptorCode(0, 1, 1)))(isTrue) &&
          assert(result.contains(DescriptorCode(0, 1, 2)))(isTrue) &&
          assert(result.contains(DescriptorCode(0, 2, 1)))(isTrue)
      },

      test("aggregateToMapTypedSorted should sort entries by line number") {
        val entry1 = createTableB(fxy = "001001", elementName = "Third", lineNumber = 15)
        val entry2 = createTableB(fxy = "001001", elementName = "First", lineNumber = 5)
        val entry3 = createTableB(fxy = "001001", elementName = "Second", lineNumber = 10)
        val expectedKey = DescriptorCode(0, 1, 1)

        for {
          result <- ZStream(entry1, entry2, entry3)
            .run(BufrTableBAggregator.aggregateToMapTypedSorted())
        } yield {
          val entries = result.get(expectedKey).get
          assert(entries.map(_.elementName))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(5, 10, 15)))
        }
      },

      test("aggregateToMapTypedPreserveOrder should maintain insertion order") {
        val entry1 = createTableB(fxy = "001001", elementName = "First", lineNumber = 10)
        val entry2 = createTableB(fxy = "001001", elementName = "Second", lineNumber = 5)
        val entry3 = createTableB(fxy = "001001", elementName = "Third", lineNumber = 15)
        val expectedKey = DescriptorCode(0, 1, 1)

        for {
          result <- ZStream(entry1, entry2, entry3)
            .run(BufrTableBAggregator.aggregateToMapTypedPreserveOrder())
        } yield {
          val entries = result.get(expectedKey).get
          // Should maintain insertion order (First, Second, Third) regardless of line numbers
          assert(entries.map(_.elementName))(equalTo(List("First", "Second", "Third"))) &&
          assert(entries.map(_.lineNumber))(equalTo(List(10, 5, 15)))
        }
      }
    ),

    suite("Error handling tests")(
      test("should fail on invalid FXY code when extracting key") {
        val invalidEntry = createTableB(fxy = "invalid")
        val stream = ZStream(invalidEntry)
        
        assertZIO(stream.run(BufrTableBAggregator.aggregateToMapTyped()).exit)(
          dies(isSubtype[IllegalArgumentException](anything))
        )
      }
    )
  )
}