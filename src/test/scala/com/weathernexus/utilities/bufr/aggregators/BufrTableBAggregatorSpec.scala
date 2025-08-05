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

      // 2. Call the toEntry function.
      val result = BufrTableBAggregator.toEntry(bufrTableB)

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

    suite("Error handling tests")(
      test("should fail on invalid FXY code when extracting key") {
        val invalidEntry = createTableB(fxy = "invalid")
        val stream = ZStream(invalidEntry)
        
        assertZIO(stream.run(BufrTableBAggregator.aggregateToMap()).exit)(
          dies(isSubtype[IllegalArgumentException](anything))
        )
      }
    ),
    `toEntry should correctly convert a BufrTableB to a BufrTableBEntry`
  )
}