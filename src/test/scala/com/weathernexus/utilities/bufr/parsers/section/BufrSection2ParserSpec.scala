package com.weathernexus.utilities.bufr.parsers.section

import zio._
import zio.stream._ 
import zio.test._

object BufrSection2ParserSpec extends ZIOSpecDefault {

  val validSection2MinimalData = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x04.toByte) ++  // Section length: 4 bytes (minimum)
    Array(0x00.toByte)                               // Reserved byte: 0
  )

  val validSection2WithLocalData = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x0A.toByte) ++  // Section length: 10 bytes
    Array(0x00.toByte) ++                            // Reserved byte: 0
    Array(0x01.toByte, 0x02.toByte, 0x03.toByte) ++  // Local data: some example bytes
    Array(0x04.toByte, 0x05.toByte, 0x06.toByte)     // More local data
  )

  val validSection2AnotherExample = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x08.toByte) ++  // Section length: 8 bytes
    Array(0x00.toByte) ++                            // Reserved byte: 0
    Array(0xAA.toByte, 0xBB.toByte, 0xCC.toByte) ++  // Different local data
    Array(0xDD.toByte)                               // More local data
  )

  val shortSection2Data = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte)                  // Only 2 bytes (need at least 4)
  )

  val invalidReservedByteData = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x04.toByte) ++  // Section length: 4 bytes
    Array(0x01.toByte)                               // Reserved byte: 1 (should be 0)
  )

  val invalidLengthTooSmallData = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x03.toByte) ++  // Section length: 3 bytes (less than minimum 4)
    Array(0x00.toByte)                               // Reserved byte: 0
  )

  val sectionLengthExceedsDataData = Chunk.fromArray(
    Array(0x00.toByte, 0x00.toByte, 0x0A.toByte) ++  // Section length: 10 bytes
    Array(0x00.toByte) ++                            // Reserved byte: 0
    Array(0x01.toByte, 0x02.toByte)                  // Only 6 bytes total (need 10)
  )

  val shouldParseMinimalSection2 = 
    test("should parse minimal Section 2 with no local data") {
      for {
        result <- BufrSection2Parser.parseSection2(validSection2MinimalData)
      } yield assertTrue(
        result.sectionLength == 4 &&
        result.reserved == 0 &&
        result.localData.isEmpty
      )
    }

  val shouldParseSection2WithLocalData = 
    test("should parse Section 2 with local data") {
      for {
        result <- BufrSection2Parser.parseSection2(validSection2WithLocalData)
      } yield assertTrue(
        result.sectionLength == 10 &&
        result.reserved == 0 &&
        result.localData.length == 6 &&
        result.localData.toArray.toSeq == Seq(0x01, 0x02, 0x03, 0x04, 0x05, 0x06)
      )
    }

  val shouldFailWithInsufficientDataForShortData = 
    test("should fail with Section2InsufficientData for short data") {
      for {
        result <- BufrSection2Parser.parseSection2(shortSection2Data).flip
      } yield assertTrue(result match {
        case Section2InsufficientData(4, 2) => true
        case _ => false
      })
    }

  val shouldFailWithInvalidReservedByte = 
    test("should fail with Section2InvalidReservedByte for non-zero reserved byte") {
      for {
        result <- BufrSection2Parser.parseSection2(invalidReservedByteData).flip
      } yield assertTrue(result match {
        case Section2InvalidReservedByte(1) => true
        case _ => false
      })
    }

  val shouldFailWithInvalidLengthWhenSectionLengthTooSmall = 
    test("should fail with Section2InvalidLength when section length is less than minimum") {
      for {
        result <- BufrSection2Parser.parseSection2(invalidLengthTooSmallData).flip
      } yield assertTrue(result match {
        case Section2InvalidLength(3, 4) => true
        case _ => false
      })
    }

  val shouldFailWithInsufficientDataWhenSectionLengthExceedsDataLength = 
    test("should fail with Section2InsufficientData when section length exceeds actual data length") {
      for {
        result <- BufrSection2Parser.parseSection2(sectionLengthExceedsDataData).flip
      } yield assertTrue(result match {
        case Section2InsufficientData(10, 6) => true
        case _ => false
      })
    }

  val section2InsufficientDataShouldFormatErrorMessageCorrectly =
    test("Section2InsufficientData should format error message correctly") {
      val error = Section2InsufficientData(10, 6)
      assertTrue(error.getMessage == "Section 2: Expected 10 bytes, got 6")
    }

  val section2InvalidLengthShouldFormatErrorMessageCorrectly =
    test("Section2InvalidLength should format error message correctly") {
      val error = Section2InvalidLength(3, 4)
      assertTrue(error.getMessage == "Section 2: Invalid length 3, minimum expected 4")
    }

  val section2InvalidReservedByteShouldFormatErrorMessageCorrectly =
    test("Section2InvalidReservedByte should format error message correctly") {
      val error = Section2InvalidReservedByte(5)
      assertTrue(error.getMessage == "Section 2: Reserved byte should be 0, got 5")
    }

  val parseSection2StreamShouldProcessMultipleSection2Messages =
    test("parseSection2Stream should process multiple Section 2 messages") {
      val inputStream = ZStream(validSection2WithLocalData, validSection2AnotherExample)
      
      for {
         results <- inputStream
           .via(BufrSection2Parser.parseSection2Stream)
           .runCollect
      } yield assertTrue(
        results.size == 2 &&
        results(0).sectionLength == 10 &&
        results(0).localData.length == 6 &&
        results(0).localData.toArray.map(_ & 0xFF).toSeq == Seq(0x01, 0x02, 0x03, 0x04, 0x05, 0x06) &&
        results(1).sectionLength == 8 &&
        results(1).localData.length == 4 &&
        results(1).localData.toArray.map(_ & 0xFF).toSeq == Seq(0xAA, 0xBB, 0xCC, 0xDD)
      )
    }

  val parseSection2StreamShouldFailOnInvalidData =
    test("parseSection2Stream should propagate parse errors") {
      val inputStream = ZStream(shortSection2Data)
      
      for {
        result <- inputStream
          .via(BufrSection2Parser.parseSection2Stream)
          .runCollect
          .flip
      } yield assertTrue(result match {
        case Section2InsufficientData(4, 2) => true
        case _ => false
      })
    }

  def spec = suite("BufrSection2Parser")(
    shouldParseMinimalSection2,
    shouldParseSection2WithLocalData,
    shouldFailWithInsufficientDataForShortData,
    shouldFailWithInvalidReservedByte,
    shouldFailWithInvalidLengthWhenSectionLengthTooSmall,
    shouldFailWithInsufficientDataWhenSectionLengthExceedsDataLength,
    section2InsufficientDataShouldFormatErrorMessageCorrectly,
    section2InvalidLengthShouldFormatErrorMessageCorrectly,
    section2InvalidReservedByteShouldFormatErrorMessageCorrectly,
    parseSection2StreamShouldProcessMultipleSection2Messages,
    parseSection2StreamShouldFailOnInvalidData
  )
}