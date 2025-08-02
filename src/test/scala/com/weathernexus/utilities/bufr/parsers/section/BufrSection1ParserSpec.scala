package com.weathernexus.utilities.bufr.parsers.section
import zio._
import zio.stream._ 
import zio.test._

object BufrSection1ParserSpec extends ZIOSpecDefault {

  val validSection1DataEdition4 = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x16.toByte) ++  // Section length: 22 bytes
    Array(0x00.toByte) ++                            // Master table number: 0
    Array(0x01.toByte, 0x02.toByte) ++               // Originating center: 258
    Array(0x03.toByte, 0x04.toByte) ++               // Originating subcenter: 772
    Array(0x05.toByte) ++                            // Update sequence: 5
    Array(0x80.toByte) ++                            // Optional section flag (bit 7 set)
    Array(0x06.toByte) ++                            // Data category: 6
    Array(0x07.toByte) ++                            // International subcategory: 7
    Array(0x08.toByte) ++                            // Local subcategory: 8
    Array(0x09.toByte) ++                            // Master table version: 9
    Array(0x0A.toByte) ++                            // Local table version: 10
    Array(0x07.toByte, 0xE7.toByte) ++               // Year: 2023
    Array(0x0C.toByte) ++                            // Month: 12
    Array(0x19.toByte) ++                            // Day: 25
    Array(0x0E.toByte) ++                            // Hour: 14
    Array(0x1E.toByte) ++                            // Minute: 30
    Array(0x2D.toByte)                               // Second: 45
  )

  val validSection1DataEdition4_second = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x16.toByte) ++  // Section length: 22 bytes
    Array(0x01.toByte) ++                            // Master table number: 1
    Array(0x00.toByte, 0x01.toByte) ++               // Originating center: 1
    Array(0x00.toByte, 0x02.toByte) ++               // Originating subcenter: 2
    Array(0x01.toByte) ++                            // Update sequence: 1
    Array(0x00.toByte) ++                            // Optional section flag (bit 7 clear)
    Array(0x01.toByte) ++                            // Data category: 1
    Array(0x02.toByte) ++                            // International subcategory: 2
    Array(0x03.toByte) ++                            // Local subcategory: 3
    Array(0x0B.toByte) ++                            // Master table version: 11
    Array(0x0C.toByte) ++                            // Local table version: 12
    Array(0x07.toByte, 0xE8.toByte) ++               // Year: 2024
    Array(0x01.toByte) ++                            // Month: 1
    Array(0x0F.toByte) ++                            // Day: 15
    Array(0x12.toByte) ++                            // Hour: 18
    Array(0x00.toByte) ++                            // Minute: 0
    Array(0x00.toByte)                               // Second: 0
  )
      
  val shortSection1Data = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x16.toByte) ++  // Section length: 22 bytes
    Array(0x00.toByte, 0x01.toByte, 0x02.toByte) ++  // Only 6 bytes total
    Array(0x03.toByte, 0x04.toByte, 0x05.toByte)
  )

  val invalidDateSection1Data = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x16.toByte) ++  // Section length: 22 bytes
    Array(0x00.toByte) ++                            // Master table number: 0
    Array(0x01.toByte, 0x02.toByte) ++               // Originating center: 258
    Array(0x03.toByte, 0x04.toByte) ++               // Originating subcenter: 772
    Array(0x05.toByte) ++                            // Update sequence: 5
    Array(0x00.toByte) ++                            // Optional section flag (bit 7 clear)
    Array(0x06.toByte) ++                            // Data category: 6
    Array(0x07.toByte) ++                            // International subcategory: 7
    Array(0x08.toByte) ++                            // Local subcategory: 8
    Array(0x09.toByte) ++                            // Master table version: 9
    Array(0x0A.toByte) ++                            // Local table version: 10
    Array(0x07.toByte, 0xE7.toByte) ++               // Year: 2023
    Array(0x0D.toByte) ++                            // Month: 13 (invalid!)
    Array(0x19.toByte) ++                            // Day: 25
    Array(0x0E.toByte) ++                            // Hour: 14
    Array(0x1E.toByte) ++                            // Minute: 30
    Array(0x2D.toByte)                               // Second: 45
  )

  val invalidTimeSection1Data = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x16.toByte) ++  // Section length: 22 bytes
    Array(0x00.toByte) ++                            // Master table number: 0
    Array(0x01.toByte, 0x02.toByte) ++               // Originating center: 258
    Array(0x03.toByte, 0x04.toByte) ++               // Originating subcenter: 772
    Array(0x05.toByte) ++                            // Update sequence: 5
    Array(0x00.toByte) ++                            // Optional section flag (bit 7 clear)
    Array(0x06.toByte) ++                            // Data category: 6
    Array(0x07.toByte) ++                            // International subcategory: 7
    Array(0x08.toByte) ++                            // Local subcategory: 8
    Array(0x09.toByte) ++                            // Master table version: 9
    Array(0x0A.toByte) ++                            // Local table version: 10
    Array(0x07.toByte, 0xE7.toByte) ++               // Year: 2023
    Array(0x0C.toByte) ++                            // Month: 12
    Array(0x19.toByte) ++                            // Day: 25
    Array(0x18.toByte) ++                            // Hour: 24 (invalid!)
    Array(0x1E.toByte) ++                            // Minute: 30
    Array(0x2D.toByte)                               // Second: 45
  )

  val shouldParseValidBUFRSection1 = 
    test("should parse valid BUFR Section 1 for Edition 4") {
      for {
        result <- BufrSection1Parser.parseSection1(validSection1DataEdition4)
      } yield assertTrue(
        result.sectionLength == 22 &&
        result.masterTableNumber == 0 &&
        result.originatingCenterId == 258 &&
        result.originatingSubcenterId == 772 &&
        result.updateSequenceNumber == 5 &&
        result.optionalSection == true &&
        result.dataCategory == 6 &&
        result.internationalDataSubcategory == 7 &&
        result.localDataSubcategory == 8 &&
        result.masterTableVersionNumber == 9 &&
        result.localTableVersionNumber == 10 &&
        result.year == 2023 &&
        result.month == 12 &&
        result.day == 25 &&
        result.hour == 14 &&
        result.minute == 30 &&
        result.second == 45
      )
    }

  val shouldFailWithInsufficientDataForShortData = 
    test("should fail with Section1InsufficientData for short data") {
      for {
        result <- BufrSection1Parser.parseSection1(shortSection1Data).flip
      } yield assertTrue(result match {
        case Section1InsufficientData(22, 9) => true
        case _ => false
      })
    }

  val shouldFailWithInvalidDateForBadMonth = 
    test("should fail with Section1InvalidDate for invalid month") {
      for {
        result <- BufrSection1Parser.parseSection1(invalidDateSection1Data).flip
      } yield assertTrue(result match {
        case Section1InvalidDate(2023, 13, 25) => true
        case _ => false
      })
    }

  val shouldFailWithInvalidTimeForBadHour = 
    test("should fail with Section1InvalidTime for invalid hour") {
      for {
        result <- BufrSection1Parser.parseSection1(invalidTimeSection1Data).flip
      } yield assertTrue(result match {
        case Section1InvalidTime(24, 30, 45) => true
        case _ => false
      })
    }

  val section1InsufficientDataShouldFormatErrorMessageCorrectly =
    test("Section1InsufficientData should format error message correctly") {
      val error = Section1InsufficientData(22, 15)
      assertTrue(error.getMessage == "Section 1: Expected 22 bytes, got 15")
    }

  val section1InvalidLengthShouldFormatErrorMessageCorrectly =
    test("Section1InvalidLength should format error message correctly") {
      val error = Section1InvalidLength(18, 22)
      assertTrue(error.getMessage == "Section 1: Invalid length 18, minimum expected 22")
    }

  val section1InvalidDateShouldFormatErrorMessageCorrectly =
    test("Section1InvalidDate should format error message correctly") {
      val error = Section1InvalidDate(2023, 13, 32)
      assertTrue(error.getMessage == "Section 1: Invalid date 2023-13-32")
    }

  val section1InvalidTimeShouldFormatErrorMessageCorrectly =
    test("Section1InvalidTime should format error message correctly") {
      val error = Section1InvalidTime(25, 61, 62)
      assertTrue(error.getMessage == "Section 1: Invalid time 25:61:62")
    }

  val parseSection1StreamShouldProcessMultipleSection1Messages =
    test("parseSection1Stream should process multiple Section 1 messages") {
      val inputStream = ZStream(validSection1DataEdition4, validSection1DataEdition4_second)
      
      for {
         results <- inputStream
           .via(BufrSection1Parser.parseSection1Stream)
           .runCollect
      } yield assertTrue(
        results.size == 2 &&
        results(0).originatingCenterId == 258 &&
        results(0).year == 2023 &&
        results(0).second == 45 &&
        results(1).originatingCenterId == 1 &&
        results(1).year == 2024 &&
        results(1).second == 0
      )
    }

  val shouldFailWithInvalidLengthWhenSectionLengthTooSmall = 
    test("should fail with Section1InvalidLength when section length is less than minimum") {
      // Create data with section length of 21 (less than 22 minimum)
      val invalidLengthData = Chunk.fromArray(
        Array(0x00.toByte, 0x00.toByte, 0x15.toByte) ++  // Section length: 21 bytes (invalid for Edition 4)
        Array(0x00.toByte) ++                            // Master table number: 0
        Array(0x01.toByte, 0x02.toByte) ++               // Originating center: 258
        Array(0x03.toByte, 0x04.toByte) ++               // Originating subcenter: 772
        Array(0x05.toByte) ++                            // Update sequence: 5
        Array(0x00.toByte) ++                            // Optional section flag
        Array(0x06.toByte) ++                            // Data category: 6
        Array(0x07.toByte) ++                            // International subcategory: 7
        Array(0x08.toByte) ++                            // Local subcategory: 8
        Array(0x09.toByte) ++                            // Master table version: 9
        Array(0x0A.toByte) ++                            // Local table version: 10
        Array(0x07.toByte, 0xE7.toByte) ++               // Year: 2023
        Array(0x0C.toByte) ++                            // Month: 12
        Array(0x19.toByte) ++                            // Day: 25
        Array(0x0E.toByte) ++                            // Hour: 14
        Array(0x1E.toByte) ++                            // Minute: 30
        Array(0x2D.toByte)                               // Second: 45
      )
      
      for {
        result <- BufrSection1Parser.parseSection1(invalidLengthData).flip
      } yield assertTrue(result match {
        case Section1InvalidLength(21, 22) => true
        case _ => false
      })
    }

  val shouldFailWithInsufficientDataWhenSectionLengthExceedsDataLength = 
    test("should fail with Section1InsufficientData when section length exceeds actual data length") {
      // Create data with section length of 30 but only provide 22 bytes
      val mismatchedLengthData = Chunk.fromArray(
        Array(0x00.toByte, 0x00.toByte, 0x1E.toByte) ++  // Section length: 30 bytes (but we only have 22)
        Array(0x00.toByte) ++                            // Master table number: 0
        Array(0x01.toByte, 0x02.toByte) ++               // Originating center: 258
        Array(0x03.toByte, 0x04.toByte) ++               // Originating subcenter: 772
        Array(0x05.toByte) ++                            // Update sequence: 5
        Array(0x00.toByte) ++                            // Optional section flag
        Array(0x06.toByte) ++                            // Data category: 6
        Array(0x07.toByte) ++                            // International subcategory: 7
        Array(0x08.toByte) ++                            // Local subcategory: 8
        Array(0x09.toByte) ++                            // Master table version: 9
        Array(0x0A.toByte) ++                            // Local table version: 10
        Array(0x07.toByte, 0xE7.toByte) ++               // Year: 2023
        Array(0x0C.toByte) ++                            // Month: 12
        Array(0x19.toByte) ++                            // Day: 25
        Array(0x0E.toByte) ++                            // Hour: 14
        Array(0x1E.toByte) ++                            // Minute: 30
        Array(0x2D.toByte)                               // Second: 45
      )
      
      for {
        result <- BufrSection1Parser.parseSection1(mismatchedLengthData).flip
      } yield assertTrue(result match {
        case Section1InsufficientData(30, 22) => true
        case _ => false
      })
    }

  def spec = suite("BufrSection1Parser")(
    shouldParseValidBUFRSection1,
    shouldFailWithInsufficientDataForShortData,
    shouldFailWithInvalidDateForBadMonth,
    shouldFailWithInvalidTimeForBadHour,
    shouldFailWithInvalidLengthWhenSectionLengthTooSmall,
    shouldFailWithInsufficientDataWhenSectionLengthExceedsDataLength,
    section1InsufficientDataShouldFormatErrorMessageCorrectly,
    section1InvalidLengthShouldFormatErrorMessageCorrectly,
    section1InvalidDateShouldFormatErrorMessageCorrectly,
    section1InvalidTimeShouldFormatErrorMessageCorrectly,
    parseSection1StreamShouldProcessMultipleSection1Messages
  )
}