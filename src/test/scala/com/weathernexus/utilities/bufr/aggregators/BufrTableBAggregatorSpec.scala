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

  val `toEntry should correctly convert a BufrTableB to a BufrTableBEntry` =
    test("toEntry should correctly convert a BufrTableB instance to a BufrTableBEntry") {
      // 1. Create a sample BufrTableB object with all fields populated.
      val bufrTableB = BufrTableB(
        classNumber = 1,
        className = "Identification",
        fxy = "001001",
        elementName = "WMO block number",
        note = Some("This is a test note"),
        bufrUnit = "Numeric",
        bufrScale = 0,
        bufrReferenceValue = 0,
        bufrDataWidth = 7,
        crexUnit = Some("Unit"),
        crexScale = Some(1),
        crexDataWidth = Some(8),
        status = Some("Operational"),
        sourceFile = "test.csv",
        lineNumber = 10
      )

      // 2. Call the toEntry function through the service.
      for {
        aggregator <- ZIO.service[BufrTableBAggregator]
        result = aggregator.toEntry(bufrTableB)
      } yield {
        // 3. Define the expected BufrTableBEntry object for comparison.
        val expectedEntry = BufrTableBEntry(
          classNumber = 1,
          className = "Identification",
          elementName = "WMO block number",
          note = Some("This is a test note"),
          bufrUnit = "Numeric",
          bufrScale = 0,
          bufrReferenceValue = 0,
          bufrDataWidth = 7,
          crexUnit = Some("Unit"),
          crexScale = Some(1),
          crexDataWidth = Some(8),
          status = Some("Operational"),
          sourceFile = "test.csv",
          lineNumber = 10
        )

        // 4. Use ZIO Test's `equalTo` assertion to verify the result.
        assert(result)(equalTo(expectedEntry))
      }
    }

  def spec = suite("BufrTableBAggregatorSpec")(
    
    // Suite to test the DescriptorCode functionality, which acts as the key
    suite("DescriptorCode tests")(
      test("should create key from constructor") {
        val key = DescriptorCode.ElementDescriptor(1, 1)
        assert(key.toFXY)(equalTo("001001")) &&
        assert(key.toString)(equalTo("001001"))
      },

      test("should parse key from FXY string") {
        val parsed = DescriptorCode.fromFXY("001001")
        val expected = DescriptorCode.ElementDescriptor(1, 1)
        assert(parsed)(isSome(equalTo(expected)))
      },

      test("should handle invalid FXY string formats") {
        assert(DescriptorCode.fromFXY("invalid"))(isNone) &&
        assert(DescriptorCode.fromFXY("01"))(isNone) &&
        assert(DescriptorCode.fromFXY("401001"))(isNone) // Invalid F value
      }
    ),

    suite("Aggregator service tests")(
      test("should aggregate using service") {
        val entries = List(
          createTableB(fxy = "001001", elementName = "WMO block number"),
          createTableB(fxy = "001002", elementName = "WMO station number"),
          createTableB(fxy = "002001", elementName = "Type of station")
        )
        
        for {
          aggregator <- ZIO.service[BufrTableBAggregator]
          result <- ZStream.fromIterable(entries)
            .run(aggregator.aggregateToMap())
        } yield {
          assert(result)(hasSize(equalTo(3))) &&
          assert(result.get(BufrTableBKey("001001")).get.head.elementName)(equalTo("WMO block number")) &&
          assert(result.get(BufrTableBKey("001002")).get.head.elementName)(equalTo("WMO station number")) &&
          assert(result.get(BufrTableBKey("002001")).get.head.elementName)(equalTo("Type of station"))
        }
      },

      test("should aggregate using convenience method") {
        val entries = List(
          createTableB(fxy = "001001", elementName = "WMO block number"),
          createTableB(fxy = "001002", elementName = "WMO station number")
        )
        
        for {
          result <- ZStream.fromIterable(entries)
            .run(BufrTableBAggregator.aggregateToMap())
        } yield {
          assert(result)(hasSize(equalTo(2))) &&
          assert(result.get(BufrTableBKey("001001")).get.head.elementName)(equalTo("WMO block number")) &&
          assert(result.get(BufrTableBKey("001002")).get.head.elementName)(equalTo("WMO station number"))
        }
      },

      test("should handle multiple entries with same FXY") {
        val entries = List(
          createTableB(fxy = "001001", elementName = "First entry", sourceFile = "file1.csv", lineNumber = 1),
          createTableB(fxy = "001001", elementName = "Second entry", sourceFile = "file2.csv", lineNumber = 2),
          createTableB(fxy = "001001", elementName = "Third entry", sourceFile = "file3.csv", lineNumber = 3)
        )
        
        for {
          aggregator <- ZIO.service[BufrTableBAggregator]
          result <- ZStream.fromIterable(entries)
            .run(aggregator.aggregateToMap())
        } yield {
          val entriesForKey = result.get(BufrTableBKey("001001")).get
          assert(result)(hasSize(equalTo(1))) &&
          assert(entriesForKey)(hasSize(equalTo(3))) &&
          // Entries are in reverse order (last inserted first)
          assert(entriesForKey.head.elementName)(equalTo("Third entry")) &&
          assert(entriesForKey(1).elementName)(equalTo("Second entry")) &&
          assert(entriesForKey(2).elementName)(equalTo("First entry"))
        }
      },

      test("should handle empty stream") {
        for {
          aggregator <- ZIO.service[BufrTableBAggregator]
          result <- ZStream.empty
            .run(aggregator.aggregateToMap())
        } yield {
          assert(result)(isEmpty)
        }
      }
    ),

    // suite("Error handling tests")(
    //   test("should fail on invalid FXY code when extracting key") {
    //     val invalidEntry = createTableB(fxy = "invalid")
        
    //     for {
    //       aggregator <- ZIO.service[BufrTableBAggregator]
    //       exit <- ZStream(invalidEntry)
    //         .run(aggregator.aggregateToMap())
    //         .exit
    //     } yield assert(exit)(dies(isSubtype[IllegalArgumentException](anything)))
    //   },

    //   test("should fail on invalid FXY code using convenience method") {
    //     val invalidEntry = createTableB(fxy = "invalid")
        
    //     for {
    //       exit <- ZStream(invalidEntry)
    //         .run(BufrTableBAggregator.aggregateToMap())
    //         .exit
    //     } yield assert(exit)(dies(isSubtype[IllegalArgumentException](anything)))
    //   }
    // )

  ).provide(
    BufrTableBAggregator.live
  )
}